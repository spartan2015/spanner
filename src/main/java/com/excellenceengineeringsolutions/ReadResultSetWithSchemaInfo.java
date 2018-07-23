

package com.excellenceengineeringsolutions;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


public class ReadResultSetWithSchemaInfo
{
  public static final String CN = "InsertExample";
  public static final String CNP = CN + ".";

  public static final String LIST_SEPARATOR = ",";
  public static final String FIELD_SEPARATOR = ";";
  public static final String ROW_SEPARATOR = "\n";

  @Test
  public void readTest()
  {
    String projectId = "a-cloud-spanner";
    SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder()
      .setProjectId(projectId)
      .setSessionPoolOption(
        SessionPoolOptions.newBuilder()
          .setFailIfPoolExhausted()
          .setMaxSessions(100)
          .setMinSessions(1)
          .setWriteSessionsFraction(0.5f)
          .build())
      .setNumChannels(1)
      .setTransportOptions(GrpcTransportOptions.newBuilder()
        .setExecutorFactory(new GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService>()
        {
          private final ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(2);

          @Override
          public ScheduledExecutorService get()
          {
            return service;
          }

          @Override
          public void release(ScheduledExecutorService service)
          {
            service.shutdown();
          }
        })
        .build());

    SpannerOptions options = spannerOptionsBuilder.build();
    Spanner spanner = null;
    try
    {
      spanner = options.getService();
      String instanceId = "eu-instance";
      String databaseId = "vasile";
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      readFromResultSet(dbClient);
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private void readFromResultSet(DatabaseClient dbClient)
  {
    String tableName = "Sequences";;
    ResultSet resultSet = dbClient.singleUse().executeQuery(
      Statement.of(String.format("select column_name,spanner_type from information_schema.columns where upper(table_name) = upper('%s')",tableName)
    ));
    List<String> columns = new ArrayList();
    StringBuilder sb = new StringBuilder();
    while (resultSet.next()){
      String column = resultSet.getString("column_name");
      columns.add(column);
      sb.append(column);
      sb.append(FIELD_SEPARATOR);
    }
    sb.deleteCharAt(sb.length()-1);
    sb.append(ROW_SEPARATOR);

    resultSet = dbClient.singleUse().read(tableName,KeySet.all(),columns,Options.limit(1000));


    while ( resultSet.next() )
    {
      readRow(resultSet, sb);
      System.out.println(readField(resultSet, 2));
    }
    System.out.println(sb.toString());

  }

  private String getFieldName(com.google.cloud.spanner.ResultSet resultSet, int i)
  {
    return resultSet.getCurrentRowAsStruct().getType().getStructFields().get(i).getName();
  }

  private void readRow(ResultSet resultSet, StringBuilder sb)
  {
    for ( Type.StructField field : resultSet.getCurrentRowAsStruct().getType().getStructFields() )
    {
      readField(sb, field, resultSet.getCurrentRowAsStruct());
      sb.append(FIELD_SEPARATOR);
    }
    sb.append(ROW_SEPARATOR);
  }

  public static String readField(ResultSet rs, int index){
    StringBuilder sb = new StringBuilder();
    readField(sb, rs.getCurrentRowAsStruct().getType().getStructFields().get(index), rs.getCurrentRowAsStruct());
    return sb.toString();
  }

  public static String readField(Type.StructField field, Struct struct){
    StringBuilder sb = new StringBuilder();
    readField(sb, field, struct);
    return sb.toString();
  }

  public static void readField(StringBuilder sb, Type.StructField field, Struct struct)
  {
    if (!struct.isNull(field.getName()))
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
