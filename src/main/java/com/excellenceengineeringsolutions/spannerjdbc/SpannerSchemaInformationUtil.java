
/*
package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider;

import com.excellenceengineeringsolutions.db.intf.ISdfHome;
import com.excellenceengineeringsolutions.db.intf.RuntimeException;
import com.excellenceengineeringsolutions.intf.fapi.IFrwComponentHandle;
import com.excellenceengineeringsolutions.intf.fapi.IFrwComponentManagementHome;
import com.excellenceengineeringsolutions.intf.fapi.IFrwContainer;
import com.excellenceengineeringsolutions.intf.fapi.IFrwSupervision;
import com.excellenceengineeringsolutions.intf.fapi.IFrwTracer;

import java.text.NumberFormat;
import java.util.Collections;


public class SpannerSchemaInformationUtil
{

  private static final String ARRAY_BEGIN = "ARRAY<";
  private static final String ARRAY_END = ">";

  public static String getIndexIfExist(IFrwComponentHandle mComponentHandle, ISdfHome mSdfHome, DatabaseClient client, String tableName, String[] indexColumns)
  {
    int currentPosition = 0;
    boolean found;
    String currentIndexName = null;

    String sqlQuery = "SELECT index_name,column_name,ordinal_position FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
      "where index_name!='PRIMARY_KEY' and upper(table_name) = @table_name order by index_name,ordinal_position";


    try ( ResultSet resultSet = client.singleUse().executeQuery(Statement.newBuilder(sqlQuery)
      .bind("table_name").to(tableName.toUpperCase())
      .build()) )
    {


      while ( resultSet.next() )
      {
        String indexName = resultSet.getString(0);
        String columnName = resultSet.getString(1);
        int position = (int) resultSet.getLong(2);

        do
        {
          found = false;
          if ( currentPosition == 0 )
          {
            currentIndexName = indexName;
          }

          if ( currentIndexName.equalsIgnoreCase(indexName) )
          {
            if ( currentPosition < indexColumns.length )
            {
              if ( columnName.equalsIgnoreCase(indexColumns[currentPosition]) && position == currentPosition + 1 )
              {
                ++currentPosition;
              } else
              {
                currentPosition = 0;
              }
            } else
            {
              currentPosition = 0;
            }
          } else
          {
            if ( currentPosition == indexColumns.length )
            {
              return currentIndexName;
            }
            currentPosition = 0;
            found = true;
          }
        } while ( found );
      }

    }
    catch ( SpannerException ex )
    {
      mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.getIndexIfExist(): ignored SQLException: " + ex.getMessage());
    }
    catch ( Exception ex )
    {
      mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.getIndexIfExist(): ignored Exception: " + ex.getMessage());
    }
    if ( currentPosition == indexColumns.length )
    {
      return currentIndexName;
    } else
    {
      return null;
    }
  }

  public static boolean checkIfIndexExist(IFrwComponentHandle mComponentHandle, ISdfHome mSdfHome, DatabaseClient client, String indexName, String tableName, String columnName, int columnPosition)
  {
    Statement.Builder statementBuilder;
    boolean found = false;

    try
    {
      StringBuilder sql;

      sql = new StringBuilder("SELECT count(*) FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
        "where index_name!='PRIMARY_KEY' " +
        "and upper(table_name) = @table_name " +
        "and upper(index_name) = @index_name ");

      if ( columnName != null )
      {
        sql.append(" and upper(column_name)=@column_name ");
      }
      if ( columnPosition > 0 )
      {
        sql.append(" and ordinal_position=@ordinal_position");
      }
      statementBuilder = Statement.newBuilder(sql.toString());
      statementBuilder.bind("table_name").to(tableName.toUpperCase());
      statementBuilder.bind("index_name").to(indexName.toUpperCase());
      if ( columnName != null )
      {
        statementBuilder.bind("column_name").to(columnName.toUpperCase());
      }

      if ( columnPosition > 0 )
      {
        statementBuilder.bind("ordinal_position").to(columnPosition);
      }

      IFrwTracer tracer = mSdfHome.getContainer().tracer();
      if ( tracer.isTraceOn(108930815987056644L, 4) )
      {
        String var10 = "SDF:DB:ServiceManagementSupport.checkIfIndexExist(): " + sql + " param[0]=" + indexName.toUpperCase() + " param[1]=" + tableName.toUpperCase() + (columnName != null ? " param[2]=" + columnName.toUpperCase() : "") + (columnPosition > 0 ? " param[3]=" + columnPosition : "");
        tracer.trace(mComponentHandle, 108930815987056644L, 4, var10);
      }

      try ( ResultSet resultSet = client.singleUse().executeQuery(statementBuilder.build()) )
      {
        if ( resultSet.next() && resultSet.getLong(0) > 0 )
        {
          found = true;
        }
      }

    }
    catch ( SpannerException ex )
    {
      mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.checkIfIndexExist(): ignored SQLException: " + ex.toString());
    }
    catch ( Exception ex )
    {
      mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.checkIfIndexExist(): ignored Exception: " + ex.getMessage());
    }

    return found;
  }

  public static String getNextFreeIndexName(IFrwComponentHandle mComponentHandle, ISdfHome mSdfHome, DatabaseClient client, String indexName, String tableName, String uniqueness)
  {
    int start = 0;
    boolean isUnique = "UNIQUE".equalsIgnoreCase(uniqueness);
    if ( uniqueness != null && uniqueness.equalsIgnoreCase("NONUNIQUE") )
    {
      start = 50;
    }

    if ( indexName != null && tableName != null && indexName.toUpperCase().startsWith("AU") && indexName.toUpperCase().endsWith(tableName.toUpperCase().substring(3)) )
    {
      String newIndexName = null;

      try
      {
        String sql = "select index_name from information_schema.indexes" +
          " where index_type='INDEX'" +
          " and upper(table_name)=@tableName" +
          " and IS_UNIQUE = @isUnique" +
          " and index_name LIKE 'AU%'" +
          " order by index_name desc";

        Statement statement = Statement.newBuilder(sql)
          .bind("tableName").to(tableName.toUpperCase())
          .bind("isUnique").to(isUnique)
          .build();

        IFrwTracer tracer = mSdfHome.getContainer().tracer();
        if ( tracer.isTraceOn(108930815987056644L, 4) )
        {
          tracer.trace(mComponentHandle, 108930815987056644L, 4, "SDF:DB:ServiceManagementSupport.getNextFreeIndexName(): " + sql);
        }

        String currentIndexName;
        try ( ResultSet resultSet = client.singleUse().executeQuery(statement) )
        {
          if ( resultSet.next() )
          {
            currentIndexName = resultSet.getString(0);
            start = Integer.parseInt(currentIndexName.substring(2, 4));
            ++start;
          } else
          {
            currentIndexName = indexName;
          }
        }
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(2);
        newIndexName = currentIndexName.substring(0, 2) + numberFormat.format((long) start) + currentIndexName.substring(4);
      }
      catch ( SpannerException ex )
      {
        mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.getNextFreeIndexName(): ignored SpannerException: " + ex.toString());
      }
      catch ( Exception ex )
      {
        mSdfHome.getContainer().supervision().notifyError(mComponentHandle, 1, 1, 1L, "ServiceManagementSupport.getNextFreeIndexName(): ignored Exception: " + ex.getMessage());
      }

      return newIndexName;
    } else
    {
      return indexName;
    }
  }

  public static boolean checkIfTableExist(DatabaseClient client, String tableName) throws RuntimeException
  {
    boolean exists;
    try
    {
      try (
        ResultSet resultSet = client
          .singleUse()
          .executeQuery(Statement
            .newBuilder("SELECT 1 FROM information_schema.tables AS t WHERE upper(t.table_name) = @tableName")
            .bind("tableName").to(tableName.toUpperCase())
            .build())
      )
      {
        exists = resultSet.next();
      }
    }
    catch ( SpannerException ex )
    {
      throw new RuntimeException("checkIfTableExist(): ignored SpannerException: " + ex.toString());
    }
    return exists;
  }

  public static boolean checkIfTableExist(IFrwContainer container, IFrwComponentHandle mComponentHandle, DatabaseClient client, String tableName) throws RuntimeException
  {
    boolean exists;
    try
    {
      if ( container.tracer().isTraceOn(108930815987056644L, 4) )
      {
        container.tracer().trace(mComponentHandle, 108930815987056644L, 4, "SDF:DB:checkIfTableExist(): " + tableName.toUpperCase());
      }

      try (
        ResultSet resultSet = client
          .singleUse()
          .executeQuery(Statement
            .newBuilder("SELECT 1 FROM information_schema.tables AS t WHERE upper(t.table_name) = @tableName")
            .bind("tableName").to(tableName.toUpperCase())
            .build())
      )
      {
        exists = resultSet.next();
      }
    }
    catch ( SpannerException ex )
    {
      container.supervision().notifyError(mComponentHandle, 1, 1, 1L, "checkIfTableExist(): ignored SpannerException: " + ex.toString());
      throw new RuntimeException(container, mComponentHandle, "checkIfTableExist(): ignored SpannerException: " + ex.toString());
    }
    return exists;
  }

  public static boolean createCompatibleColumn(IFrwContainer container, IFrwComponentHandle componentHandle, com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider clientProvider, String providedType, int providedMaxLength, String sColumnName, String sTableName, boolean bOnlyCheck, int nVersion)
  {
    IFrwTracer tracer = container.tracer();

    if ( (providedType.equals("OnCounter") || providedType.equals("StCounter") || providedType.equals("Counter")) && !sColumnName.substring(1, 2).equals("0") )
    {
      // Counter haben inzwischen eine 0 zwischen U und Feldnamen
      sColumnName = sColumnName.substring(0, 1) + "0" + sColumnName.substring(1, Math.min(28, sColumnName.length()));
    } else if ( (providedType.equals("C0Array") || providedType.equals("CounterArray")) && !sColumnName.substring(1, 2).equals("0") )
    {
      // CounterArrays haben inzwischen eine 0 zwischen U und Feldnamen
      sColumnName = sColumnName.substring(0, 1) + "0" + sColumnName.substring(1, Math.min(28, sColumnName.length()));
    }

    boolean bFieldFound = false;
    String dbColumnType = null;
    int dbColumnLength = 0;

    if ( tracer.isTraceOn(IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM) )
    {
      String sMessage = "SDF:DB:ServiceManagementSupport.createCompatibleColumn(): for table=" + sTableName.toUpperCase() + " param[1]=" + sColumnName.toUpperCase();
      tracer.trace(componentHandle, IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM, sMessage);
    }
    try (
      ResultSet rs = clientProvider.getDatabaseClient()
        .singleUse()
        .executeQuery(
          Statement.newBuilder("select SPANNER_TYPE from information_schema.columns where upper(table_name) = @tableName and upper(column_name)=@columnName")
            .bind("tableName").to(sTableName.toUpperCase())
            .bind("columnName").to(sColumnName.toUpperCase())
            .build()
        )
    )
    {
      if ( rs.next() )
      {
        bFieldFound = true;
        SpannerType spannerType = parseType(rs.getString("SPANNER_TYPE"));
        dbColumnType = spannerType.getType();
        dbColumnLength = spannerType.getLength();
      }
    }
    catch ( SpannerException e )
    {
      tracer.trace(componentHandle, IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM, e.toString());
    }

    try
    {
      String sExpectedType;
      int nExpectedLength;
      String sNewType;

      if ( providedType.equals("Int") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<Int>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("String") )
      {
        sExpectedType = "STRING";
        nExpectedLength = providedMaxLength;
        sNewType = "STRING(" + providedMaxLength + ")";
      } else if ( providedType.equals("ARRAY<String>") )
      {
        sExpectedType = "ARRAY<STRING>";
        nExpectedLength = providedMaxLength;
        sNewType = "ARRAY<STRING(" + providedMaxLength + ")>";
      } else if ( providedType.equals("StringCrypted") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 16;
        sNewType = "STRING(16)";
      } else if ( providedType.equals("ARRAY<StringCrypted>") )
      {
        sExpectedType = "ARRAY<STRING(16)>";
        nExpectedLength = 16;
        sNewType = "ARRAY<STRING(16)>";
      } else if ( providedType.equals("StringCryptedLarge") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 16;
        sNewType = "STRING(16)";
      } else if ( providedType.equals("ARRAY<StringCryptedLarge>") )
      {
        sExpectedType = "ARRAY<STRING(16)>";
        nExpectedLength = 16;
        sNewType = "ARRAY<STRING(16)>";
      } else if ( providedType.equals("StringUnicoded") )
      {
        sExpectedType = "STRING";
        nExpectedLength =  providedMaxLength;
        sNewType = "STRING(" + providedMaxLength + ")";
      } else if ( providedType.equals("ARRAY<StringUnicoded>") )
      {
        sExpectedType = "ARRAY<STRING>";
        nExpectedLength = providedMaxLength;
        sNewType = "ARRAY<STRING(" + providedMaxLength + ")>";
      } else if ( providedType.equals("StringCryptedUnicoded") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 16;
        sNewType = "STRING(16)";
      } else if ( providedType.equals("ARRAY<StringCryptedUnicoded>") )
      {
        sExpectedType = "ARRAY<STRING(16)>";
        nExpectedLength = 16;
        sNewType = "ARRAY<STRING(16)>";
      } else if ( providedType.equals("StringLarge") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 0;
        sNewType = "STRING(MAX)";
      } else if ( providedType.equals("ARRAY<StringLarge>") )
      {
        sExpectedType = "ARRAY<STRING(MAX)>";
        nExpectedLength = 0;
        sNewType = "ARRAY<STRING(MAX)>";
      } else if ( providedType.equals("StringLargeUnicoded") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 0;
        sNewType = "STRING(MAX)";
      } else if ( providedType.equals("ARRAY<StringLargeUnicoded>") )
      {
        sExpectedType = "ARRAY<STRING(MAX)>";
        nExpectedLength = 0;
        sNewType = "ARRAY<STRING(MAX)>";
      } else if ( providedType.equals("StringCryptedLargeUnicoded") )
      {
        sExpectedType = "STRING";
        nExpectedLength = 16;
        sNewType = "STRING(16)";
      } else if ( providedType.equals("ARRAY<StringCryptedLargeUnicoded>") )
      {
        sExpectedType = "ARRAY<STRING(16)>";
        nExpectedLength = 16;
        sNewType = "ARRAY<STRING(16)>";
      } else if ( providedType.equals("Long") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<Long>") || providedType.equals("ARRAY<INT64>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("Boolean") )
      {
        sExpectedType = "BOOL";
        nExpectedLength = 0;
        sNewType = "BOOL";
      } else if ( providedType.equals("ARRAY<Boolean>") || providedType.equals("ARRAY<BOOL>") )
      {
        sExpectedType = "ARRAY<BOOL>";
        nExpectedLength = 0;
        sNewType = "ARRAY<BOOL>";
      } else if ( providedType.equals("Date") )
      {
        sExpectedType = "TIMESTAMP";
        nExpectedLength = 0;
        sNewType = "TIMESTAMP";
      } else if ( providedType.equals("ARRAY<Date>") || providedType.equals("ARRAY<TIMESTAMP>") )
      {
        sExpectedType = "ARRAY<TIMESTAMP>";
        nExpectedLength = 0;
        sNewType = "ARRAY<TIMESTAMP>";
      } else if ( providedType.equals("Double") )
      {
        sExpectedType = "FLOAT64";
        nExpectedLength = 0;
        sNewType = "FLOAT64";
      } else if ( providedType.equals("ARRAY<Double>") || providedType.equals("ARRAY<FLOAT64>") )
      {
        sExpectedType = "ARRAY<FLOAT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<FLOAT64>";
      } else if ( providedType.equals("Float") )
      {
        sExpectedType = "FLOAT64";
        nExpectedLength = 0;
        sNewType = "FLOAT64";
      } else if ( providedType.equals("ARRAY<Float>") )
      {
        sExpectedType = "ARRAY<FLOAT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<FLOAT64>";
      } else if ( providedType.equals("Account") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<Account>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("OnCounter") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<OnCounter>") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("StCounter") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<StCounter>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("C0Array") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<C0Array>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("C1Array") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<C1Array>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("C2Array") )
      {
        sExpectedType = "BYTES";
        nExpectedLength = 0;
        sNewType = "BYTES(MAX)";
      } else if ( providedType.equals("ARRAY<C2Array>") )
      {
        sExpectedType = "ARRAY<BYTES(4000)>";
        nExpectedLength = 4000;
        sNewType = "ARRAY<BYTES(4000)>";
      } else if ( providedType.equals("Counter") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<Counter>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("CounterArray") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<CounterArray>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("CounterArray1") )
      {
        sExpectedType = "INT64";
        nExpectedLength = 0;
        sNewType = "INT64";
      } else if ( providedType.equals("ARRAY<CounterArray1>") )
      {
        sExpectedType = "ARRAY<INT64>";
        nExpectedLength = 0;
        sNewType = "ARRAY<INT64>";
      } else if ( providedType.equals("CounterArray2") )
      {
        sExpectedType = "BYTES(4000)";
        nExpectedLength = 4000;
        sNewType = "BYTES(4000)";
      } else if ( providedType.equals("ARRAY<CounterArray2>") )
      {
        sExpectedType = "ARRAY<BYTES(4000)>";
        nExpectedLength = 4000;
        sNewType = "ARRAY<BYTES(4000)>";
      } else if ( providedType.equals("Raw") )
      {
        sExpectedType = "BYTES";
        nExpectedLength = 0;
        sNewType = "BYTES(MAX)";
      } else if ( providedType.equals("ARRAY<Raw>") )
      {
        sExpectedType = "ARRAY<BYTES(2000)>";
        nExpectedLength = 2000;
        sNewType = "ARRAY<BYTES(2000)>";
      }
      // ------------------------------------------------------------------------
      else
      {
        return (false);
      }

      if ( bFieldFound )
      {
        if ( tracer.isTraceOn(IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM) )
        {
          String sMessage = "SDF:DB:ServiceManagementSupport.createCompatibleColumn(): column '" + sColumnName + "' found sColumnType=" + dbColumnType + " nColumnLength=" + dbColumnLength;
          tracer.trace(componentHandle, IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM, sMessage);
        }

        if ( !bOnlyCheck )
        {
          // new comment version 2
          // overwrite comment on column
          //setDbComment(transactionManager,sColumnName,sTableName,nVersion,sType);
        }

        // check column type, length and precision
        // ---------------------------------------
        if ( sExpectedType.equals(dbColumnType) )
        {
          if ( providedType.equals("String") || providedType.equals("StringCrypted") || providedType.equals("StringCryptedLarge") || providedType.equals("StringUnicoded") || providedType.equals("StringCryptedUnicoded") )
          {
            if ( bOnlyCheck )
            {
              return (true);
            } else
            {
              if ( nExpectedLength > dbColumnLength )
              {
                // Adjust the string length as not long enough
                try
                {
                  String sSqlCmd = "alter table " + sTableName + " alter column " + sColumnName + " " + sNewType + "";
                  executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
                }
                catch ( SpannerException e )
                {
                  return (false);
                }
              }
              return (true);
            }
          } else if ( providedType.equals("StringLarge") || providedType.equals("StringLargeUnicoded") )
          {
            return (true);
          } else if ( providedType.equals("Boolean") || providedType.equals("Date") )
          {
            return (true);
          } else if ( providedType.equals("Int") )
          {
            return (true);
          } else if ( providedType.equals("Long") || providedType.equals("Account") || providedType.equals("Counter") || providedType.equals("OnCounter") || providedType.equals("StCounter") || providedType.equals("Double") || providedType.equals("Float") )
          {
            return (true);
          } else if ( providedType.equals("C0Array") )
          {
            // Check for 2nd column
            return (createCompatibleColumn(container, componentHandle, clientProvider, "C1Array", 0, sColumnName.substring(0, 1) + "1" + sColumnName.substring(2), sTableName, bOnlyCheck, nVersion));
          } else if ( providedType.equals("C1Array") )
          {
            // Check for 3rd column
            return (createCompatibleColumn(container, componentHandle, clientProvider, "C2Array", 0, sColumnName.substring(0, 1) + "2" + sColumnName.substring(2), sTableName, bOnlyCheck, nVersion));

          } else if ( providedType.equals("C2Array") )
          {
            return (true);
          } else if ( providedType.equals("CounterArray") )
          {
            return (createCompatibleColumn(container, componentHandle, clientProvider, "CounterArray1", 0, sColumnName.substring(0, 1) + "1" + sColumnName.substring(2), sTableName, bOnlyCheck, nVersion));
          } else if ( providedType.equals("CounterArray1") )
          {
            return (createCompatibleColumn(container, componentHandle, clientProvider, "CounterArray2", 0, sColumnName.substring(0, 1) + "2" + sColumnName.substring(2), sTableName, bOnlyCheck, nVersion));
          } else if ( providedType.equals("CounterArray2") )
          {
            return (true);
          } else if ( providedType.equals("Raw") )
          {
            return (true);
          } else
          {
            return nExpectedLength == dbColumnLength;
          }
        } else
        {
          return (false);
        }
      } else
      {
        if ( bOnlyCheck )
        {
          return (true);
        } else
        {
          try
          {
            String sSqlCmd;
            if ( providedType.equals("C0Array") || providedType.equals("CounterArray") )
            {
              sSqlCmd = "alter table " + sTableName + " add column " + sColumnName + " " + sNewType + "";
              executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
              sSqlCmd = "alter table " + sTableName + " add column " + sColumnName.substring(0, 1) + "1" + sColumnName.substring(2) + " INT64";
              executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
              sSqlCmd = "alter table " + sTableName + " add column " + sColumnName.substring(0, 1) + "2" + sColumnName.substring(2) + " BYTES(MAX)";
              executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
            } else if ( providedType.equals("Raw") )
            {
              sSqlCmd = "alter table " + sTableName + " add column " + sColumnName + " " + sNewType + "(MAX)";
              executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
            } else
            {
              sSqlCmd = "alter table " + sTableName + " add column " + sColumnName + " " + sNewType + "";
              executeCreateCompatibleColumnDddl(container, componentHandle, clientProvider, sSqlCmd);
            }
          }
          catch ( SpannerException e )
          {
            return (false);
          }
          //setDbComment(transactionManager,sColumnName,sTableName,nVersion,providedType);
          return (true);
        }
      }
    }
    catch ( SpannerException e )
    {
      return false;
    }
  }

  public static void executeCreateCompatibleColumnDddl(IFrwContainer container, IFrwComponentHandle componentHandle, SpannerClientProvider clientProvider, String sSqlCmd)
  {
    if ( container.tracer().isTraceOn(IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM) )
    {
      String sMessage = "SDF:DB:ServiceManagementSupport.createCompatibleColumn(): " + sSqlCmd;
      container.tracer().trace(componentHandle, IFrwComponentManagementHome.INSTALL_TRACE_TOPIC, IFrwTracer.TLEVEL_MEDIUM, sMessage);
    }
    Operation op = clientProvider.getAdminClient().updateDatabaseDdl(
      clientProvider.getInstanceId(),
      clientProvider.getDatabase(),
      Collections.singletonList(sSqlCmd), null
    ).waitFor();

    if ( !op.isSuccessful() )
    {
      String sMessage = "SDF:DB:ServiceManagementSupport.executeDdl() CRITICAL FAILED: " + op.getResult().toString();
      container.supervision().notifyError(componentHandle, IFrwSupervision.ERROR_APPLICATION, IFrwSupervision.SEVERITY_MINOR, 59, sMessage);
    }
  }

  static SpannerType parseType(String actualType)
  {
    if(actualType == null)
    {
      throw new IllegalArgumentException("actualType can not be null");
    }
    actualType = actualType.trim();
    if("".equals(actualType))
    {
      throw new IllegalArgumentException("actualType can not be blank");
    }
    final boolean isArray = actualType.startsWith(ARRAY_BEGIN) && actualType.endsWith(ARRAY_END);
    if(isArray)
    {
      actualType = actualType.substring(ARRAY_BEGIN.length(), actualType.lastIndexOf(ARRAY_END));
    }
    final int notFoundIndex = -1;
    final int openParanthesisIndex = actualType.indexOf('(');
    final boolean actualHasTypeSize = openParanthesisIndex != notFoundIndex;
    final int closingParanthesisIndex =
      actualHasTypeSize ? actualType.indexOf(')') : notFoundIndex;

    if ( actualHasTypeSize &&
      // no ')'
      (closingParanthesisIndex == notFoundIndex
        // more then one ')'
        || closingParanthesisIndex != actualType.lastIndexOf(')')
        // something after ')'
        || !actualType.substring(closingParanthesisIndex + 1).trim().isEmpty()
      )
      )
    {
      throw new IllegalArgumentException(String.format(
        "Wrong format of database column type. " +
          "Expected format is 'COLUMNTYPE(SIZE)' or 'ARRAY<COLUMNTYPE(SIZE)>' but was '%s'",
        actualType
      ));
    }

    int actualTypeSize = 0;
    if ( actualHasTypeSize )
    {
      String type = actualType.substring(openParanthesisIndex + 1, closingParanthesisIndex).trim();
      if ( !"max".equalsIgnoreCase(type) )
      {
        actualTypeSize = Integer.parseInt(type);
      }
    }
    if(actualTypeSize < 0){
      throw new IllegalArgumentException(String.format(
              "Size can not be negative" +
                      " But was '%d' for type '%s'",
              actualTypeSize, actualType
      ));
    }
     String actualTypeName =
      (actualHasTypeSize ? actualType.substring(0, openParanthesisIndex) : actualType).trim();
    if(isArray)
    {
      actualTypeName = ARRAY_BEGIN + actualTypeName + ARRAY_END;
    }

    return new SpannerType(actualTypeName, actualTypeSize);
  }

  static class SpannerType
  {
    private final String type;
    private final int length;

    SpannerType(String type, int length)
    {
      this.type = type;
      this.length = length;
    }

    String getType()
    {
      return type;
    }

    int getLength()
    {
      return length;
    }
  }

}
*/
