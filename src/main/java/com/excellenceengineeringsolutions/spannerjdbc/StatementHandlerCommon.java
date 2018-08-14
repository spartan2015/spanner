//  Source file: P:/advantage/com.excellenceengineeringsolutions/StatementHandler.java

/*
 * Copyright (c) Optiva Inc. 2000 - 2018  All Rights Reserved
 * The reproduction, transmission or use of this document or
 * its contents is not permitted without express written
 * authority. Offenders will be liable for damages. All rights,
 * including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 * Technical modifications possible.
 * Technical specifications and features are binding only
 * insofar as they are specifically and expressly agreed upon
 * in a written contract.
 */

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class StatementHandlerCommon
{
  public static final String CN = "StatementHandlerCommon";
  public static final String CNP = CN + ".";
  public static final String classVersion = "@(#) de.siemens.advantage.platform.batch.batch.impl.spanner" +
    ".StatementHandlerCommon.java : /main/br_PG931/3 : ";

  public static final String TABLE_NAME_REG_EXP = "[a-z_A-Z0-9]+";
  public static final String DYNAMIC = "#dynamic#";

  public static void setToBuilderFromResultSet(Key.Builder builder, ResultSet rs, String fieldName)
  {
    Type.Code code = rs.getCurrentRowAsStruct().getColumnType(fieldName).getCode();
    Struct struct = rs.getCurrentRowAsStruct();
    switch ( code )
    {
      case BOOL:
        builder.append(struct.getBoolean(fieldName));
        break;
      case DATE:
        builder.append(struct.getDate(fieldName));
        break;
      case BYTES:
        builder.append(struct.getBytes(fieldName));
        break;
      case INT64:
        builder.append(struct.getLong(fieldName));
        break;
      case STRING:
        builder.append(struct.getString(fieldName));
        break;
      case FLOAT64:
        builder.append(struct.getDouble(fieldName));
        break;
      case TIMESTAMP:
        builder.append(struct.getTimestamp(fieldName));
        break;
      default:
        break;
    }
  }

  public static void setToBuilderFromResultSet(Mutation.WriteBuilder builder, ResultSet rs, String fieldName)
  {
    Type.Code code = rs.getCurrentRowAsStruct().getColumnType(fieldName).getCode();
    Struct struct = rs.getCurrentRowAsStruct();
    switch ( code )
    {
      case BOOL:
        builder.set(fieldName).to(struct.getBoolean(fieldName));
        break;
      case DATE:
        builder.set(fieldName).to(struct.getDate(fieldName));
        break;
      case BYTES:
        builder.set(fieldName).to(struct.getBytes(fieldName));
        break;
      case INT64:
        builder.set(fieldName).to(struct.getLong(fieldName));
        break;
      case STRING:
        builder.set(fieldName).to(struct.getString(fieldName));
        break;
      case FLOAT64:
        builder.set(fieldName).to(struct.getDouble(fieldName));
        break;
      case TIMESTAMP:
        builder.set(fieldName).to(struct.getTimestamp(fieldName));
        break;
      default:
        break;
    }
  }

  public static Object readField(ResultSet rs, String fieldName)
  {
    Type.Code code = rs.getCurrentRowAsStruct().getColumnType(fieldName).getCode();
    Struct struct = rs.getCurrentRowAsStruct();
    switch ( code )
    {
      case BOOL:
        return struct.getBoolean(fieldName);
      case DATE:
        return struct.getDate(fieldName);
      case BYTES:
        return struct.getBytes(fieldName);
      case INT64:
        return struct.getLong(fieldName);
      case STRING:
        return struct.getString(fieldName);
      case FLOAT64:
        return struct.getDouble(fieldName);
      case TIMESTAMP:
        return struct.getTimestamp(fieldName);
      default:
        throw new IllegalStateException("type code currently not supported: " + code);
    }
  }

  static String spannerQueryParams(String select)
  {
    AtomicInteger paramIndex = new AtomicInteger(1);
    return spannerQueryParams(select, paramIndex);
  }

  static String spannerQueryParams(String select, AtomicInteger paramIndex)
  {
    StringBuilder sb = new StringBuilder(select.length());
    int fromIndex = 0;
    int foundIndex = -1;
    do
    {
      foundIndex = select.indexOf('?', fromIndex);
      sb.append(select.substring(fromIndex, foundIndex == -1 ? select.length() : foundIndex));
      if ( foundIndex != -1 )
      {
        sb.append("@_").append(paramIndex.getAndIncrement());
        fromIndex = foundIndex + 1;
      }
    } while ( foundIndex != -1 );
    return sb.toString();
  }

  static void bind(Statement.Builder builder, int paramNo, Object paramValue, Class<?> clazz)
  {
    String param = "@_" + paramNo;
    bind(builder, param, paramValue, clazz);
  }

  static void bind(Statement.Builder builder, String param, Object paramValue, Class<?> clazz)
  {
    if ( paramValue == null )
    {
      throw new IllegalArgumentException(String.format("Required param value for [%s] is missing or Null param value is not supported.", param));
    }
    if ( clazz == Boolean.class )
    {
      builder.bind(param).to((Boolean) paramValue);
    } else if ( clazz == Float.class )
    {
      builder.bind(param).to((Float) paramValue);
    } else if ( clazz == Double.class )
    {
      builder.bind(param).to((Double) paramValue);
    } else if ( clazz == Number.class )
    {
      builder.bind(param).to(((Number) paramValue).longValue());
    } else if ( clazz == String.class )
    {
      builder.bind(param).to((String) paramValue);
    } else if ( clazz == ByteArray.class )
    {
      builder.bind(param).to((ByteArray) paramValue);
    } else if ( clazz == byte[].class )
    {
      builder.bind(param).to(ByteArray.copyFrom((byte[]) paramValue));
    } else if ( clazz == Timestamp.class )
    {
      builder.bind(param).to((Timestamp) paramValue);
    } else if ( clazz == Date.class )
    {
      builder.bind(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  public static void bind(Statement.Builder builder, int paramNo, Object paramValue)
  {
    String param = "_" + paramNo;
    bind(builder, param, paramValue);
  }

  public static void bind(Statement.Builder builder, String param, Object paramValue)
  {
    if ( paramValue == null )
    {
      throw new IllegalArgumentException(String.format("Required param value for [%s] is missing or Null param value is not supported.", param));
    }
    if ( paramValue instanceof Boolean )
    {
      builder.bind(param).to((Boolean) paramValue);
    } else if ( paramValue instanceof Float )
    {
      builder.bind(param).to((Float) paramValue);
    } else if ( paramValue instanceof Double )
    {
      builder.bind(param).to((Double) paramValue);
    } else if ( paramValue instanceof Number )
    {
      builder.bind(param).to(((Number) paramValue).longValue());
    } else if ( paramValue instanceof String )
    {
      builder.bind(param).to((String) paramValue);
    } else if ( paramValue instanceof ByteArray )
    {
      builder.bind(param).to((ByteArray) paramValue);
    } else if ( paramValue instanceof byte[] )
    {
      builder.bind(param).to(ByteArray.copyFrom((byte[]) paramValue));
    } else if ( paramValue instanceof Timestamp )
    {
      builder.bind(param).to((Timestamp) paramValue);
    } else if ( paramValue instanceof Date )
    {
      builder.bind(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  static void bindParam(Mutation.WriteBuilder builder, String param, Object paramValue)
  {
    if ( paramValue == null )
    {
      builder.set(param).to((Boolean) null); // intentional
      return;
    }
    if ( paramValue instanceof Boolean )
    {
      builder.set(param).to((Boolean) paramValue);
    } else if ( paramValue instanceof Float )
    {
      builder.set(param).to((Float) paramValue);
    } else if ( paramValue instanceof Double )
    {
      builder.set(param).to((Double) paramValue);
    } else if ( paramValue instanceof Number )
    {
      builder.set(param).to(((Number) paramValue).longValue());
    } else if ( paramValue instanceof String )
    {
      builder.set(param).to((String) paramValue);
    } else if ( paramValue instanceof ByteArray )
    {
      builder.set(param).to((ByteArray) paramValue);
    } else if ( paramValue instanceof byte[] )
    {
      builder.set(param).to(ByteArray.copyFrom((byte[]) paramValue));
    } else if ( paramValue instanceof Timestamp )
    {
      builder.set(param).to((Timestamp) paramValue);
    } else if ( paramValue instanceof Date )
    {
      builder.set(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  static void bindParam(Mutation.WriteBuilder builder, String param, Object paramValue, Class<?> clazz)
  {
    if ( paramValue == null )
    {
      builder.set(param).to((Boolean) null); // intentional
      return;
    }
    if ( clazz == Boolean.class )
    {
      builder.set(param).to((Boolean) paramValue);
    } else if ( clazz == Float.class )
    {
      builder.set(param).to((Float) paramValue);
    } else if ( clazz == Double.class )
    {
      builder.set(param).to((Double) paramValue);
    } else if ( clazz == Number.class )
    {
      builder.set(param).to(((Number) paramValue).longValue());
    } else if ( clazz == String.class )
    {
      builder.set(param).to((String) paramValue);
    } else if ( clazz == ByteArray.class )
    {
      builder.set(param).to((ByteArray) paramValue);
    } else if ( clazz == byte[].class )
    {
      builder.set(param).to(ByteArray.copyFrom((byte[]) paramValue));
    } else if ( clazz == Timestamp.class )
    {
      builder.set(param).to((Timestamp) paramValue);
    } else if ( clazz == Date.class )
    {
      builder.set(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  static Object parseValue(String value)
  {
    if ( value.length() == 0 )
    {
      return null;
    } else if ( value.startsWith("'") )
    {
      return value.substring(1, value.length() - 1);
    }else if(value.startsWith("time:") ){
      return Timestamp.of(new java.util.Date(Long.valueOf(value.substring(5, value.length()))));
    }
    else if ( value.matches("\\d+") )
    {
      return Long.parseLong(value);
    } else
    {
      return value;
    }
  }

  static String join(List<String> selectKeys)
  {
    if ( selectKeys.isEmpty() )
    {
      return DYNAMIC;
    }
    StringBuilder sb = new StringBuilder();
    for ( String s : selectKeys )
    {
      sb.append(s).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public void bind(Mutation.WriteBuilder builder, Map<String, Object> parameters)
  {
    for ( String param : parameters.keySet() )
    {
      Object paramValue = parameters.get(param);
      bindParam(builder, param, paramValue);
    }
  }

  public static List<String> getKeys(TransactionContext transaction, String tableName)
  {
    List<String> keyColumns = new ArrayList<>();
    ResultSet rs = transaction.executeQuery(Statement.of(String.format("select column_name from INFORMATION_SCHEMA.INDEX_COLUMNS " +
      "where index_type='PRIMARY_KEY' and table_name = 'ATADH_CorrEventsStatTabl'",tableName)));
    while(rs.next()){
      keyColumns.add(rs.getString(0));
    }
    return keyColumns;
  }
}
