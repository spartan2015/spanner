

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.excellenceengineeringsolutions.spanner.StatementInsertHandler;
import com.excellenceengineeringsolutions.spanner.StatementSelectHandler;
import com.excellenceengineeringsolutions.db.intf.SdfException;

import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.excellenceengineeringsolutions.spanner.SpannerGenericResultSetRead.readFieldAsString;
import static com.excellenceengineeringsolutions.spanner.StatementDeleteHandler.spannerDeleteBuilder;
import static com.excellenceengineeringsolutions.spanner.StatementInsertHandler.spannerInsertBuilder;
import static com.excellenceengineeringsolutions.spanner.StatementSelectHandler.spannerQueryBuilder;
import static com.excellenceengineeringsolutions.spanner.StatementUpdateHandler.spannerUpdateBuilder;

/**
 */
public class StatementMain
{
  public static final String CN = "StatementMain";
  public static final String CNP = CN + ".";

  public static void main(String[] args) throws Exception
  {
    try
    {
      String instanceId = args[0];
      String databaseId = args[1];
      String statement = args[2];

      execute(instanceId, databaseId, statement);
    }catch(Exception ex){
      System.out.println("Usage parameters: instance db any-query");
      ex.printStackTrace();
    }
  }

  public static void execute(String instanceId, String databaseId, String statement) throws SdfException
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

      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

      DatabaseClient dbClient = spanner.getDatabaseClient(db);



      String upperCase = statement.toUpperCase();
      if ( upperCase.startsWith("INSERT") )
      {
        StatementInsertHandler.InsertMutationHolder insertMutationHolder = spannerInsertBuilder(statement, new HashMap());
        insertMutationHolder.execute(dbClient);
      } else if ( upperCase.startsWith("SELECT") )
      {
        StatementSelectHandler.SelectStatementHolder selectStatementHolder = spannerQueryBuilder(statement);
        ResultSet rs = selectStatementHolder.execute(dbClient);
        boolean headerPrinted = false;
        while ( rs.next() )
        {
          if ( !headerPrinted )
          {
            printHeader(dbClient, rs);
            headerPrinted = true;
          }
          for ( int i = 0; i < rs.getColumnCount(); i++ )
          {
            System.out.print(readFieldAsString(rs, i) + " \t");
          }
          System.out.println();
        }
      } else if ( upperCase.startsWith("DELETE") )
      {
        spannerDeleteBuilder(statement).execute(dbClient);
      } else if ( upperCase.startsWith("UPDATE") )
      {
        spannerUpdateBuilder(statement).execute(dbClient);
      }
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private static void printHeader(DatabaseClient dbClient, ResultSet rs)
  {
    for ( int i = 0; i < rs.getColumnCount(); i++ )
    {
      String name = rs.getCurrentRowAsStruct().getType().getStructFields().get(i).getName();
      System.out.println(name + "\t");
    }
  }


}
