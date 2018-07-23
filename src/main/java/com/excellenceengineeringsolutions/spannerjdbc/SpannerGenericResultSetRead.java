

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

/**
 * Generic Spanner ResultSet reader
 */
public class SpannerGenericResultSetRead
{


  public static final String LIST_SEPARATOR = ",";
  public static final String FIELD_SEPARATOR = ";";
  public static final String ROW_SEPARATOR = "\n";

  /**
   * @param resultSet
   * @param i         - 0 based
   * @return
   */
  public static String getFieldName(com.google.cloud.spanner.ResultSet resultSet, int i)
  {
    return resultSet.getCurrentRowAsStruct().getType().getStructFields().get(i).getName();
  }

  /**
   * 0 is the first field in resultset
   *
   * @param rs
   * @param index
   * @return
   */
  public static String readFieldAsString(ResultSet rs, int index)
  {
    StringBuilder sb = new StringBuilder();
    readFieldAsString(sb, rs.getCurrentRowAsStruct().getType().getStructFields().get(index), rs.getCurrentRowAsStruct());
    return sb.toString();
  }

  public static String readFieldAsString(Type.StructField field, Struct struct)
  {
    StringBuilder sb = new StringBuilder();
    readFieldAsString(sb, field, struct);
    return sb.toString();
  }

  public static void readFieldAsString(StringBuilder sb, Type.StructField field, Struct struct)
  {
    if ( !struct.isNull(field.getName()) )
    {
      Type type = field.getType();
      switch ( type.getCode() )
      {
        case ARRAY:
          sb.append("ARRAY<");
          readListField(sb, field, struct, type.getArrayElementType().getCode());
          sb.append(">");
          break;
        default:
          readSimpleField(sb, field, struct, type.getCode());
          break;
      }
    }
  }

  public static void readListField(StringBuilder sb, Type.StructField field, Struct struct, Type.Code code)
  {
    switch ( code )
    {
      case BOOL:
      {
        for ( Boolean element : struct.getBooleanList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case DATE:
      {
        for ( com.google.cloud.Date element : struct.getDateList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case BYTES:
      {
        for ( ByteArray element : struct.getBytesList(field.getName()) )
        {
          sb.append(element.toBase64()).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case INT64:
      {
        for ( Long element : struct.getLongList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case STRING:
      {
        for ( String element : struct.getStringList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case FLOAT64:
      {
        for ( Double element : struct.getDoubleList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case TIMESTAMP:
      {
        for ( Timestamp element : struct.getTimestampList(field.getName()) )
        {
          sb.append(element).append(LIST_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      case STRUCT:
      {
        for ( Struct element : struct.getStructList(field.getName()) )
        {
          for ( Type.StructField structTypeField : element.getType().getStructFields() )
          {
            readSimpleField(sb, field, element, structTypeField.getType().getCode());
            sb.append(LIST_SEPARATOR);
          }
        }
        sb.deleteCharAt(sb.length() - 1);
        break;
      }
      default:
        break;
    }
  }

  public static void readSimpleField(StringBuilder sb, Type.StructField field, Struct struct, Type.Code code)
  {
    switch ( code )
    {
      case BOOL:
        sb.append(Boolean.toString(struct.getBoolean(field.getName())));
        break;
      case DATE:
        sb.append(struct.getDate(field.getName()).toString());
        break;
      case BYTES:
        sb.append(struct.getBytes(field.getName()).toBase64());
        break;
      case INT64:
        sb.append(Long.toString(struct.getLong(field.getName())));
        break;
      case STRING:
        sb.append(struct.getString(field.getName()));
        break;
      case FLOAT64:
        sb.append(Double.toString(struct.getDouble(field.getName())));
        break;
      case TIMESTAMP:
        sb.append(struct.getTimestamp(field.getName()).toString());
        break;
      default:
        break;
    }
  }
}
