

package com.excellenceengineeringsolutions;

import com.google.cloud.RetryOption;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;


public class DdlTest
{

  @Test
  public void altertableonly()
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

      Operation operation = spanner.getDatabaseAdminClient()
              .updateDatabaseDdl(instanceId,databaseId,ImmutableList.of("alter table arrayofstring add column theNewBlob BYTES(MAX)"),null);

      //List<Operation> operations = new CopyOnWriteArrayList<>(); // when yould you know the last op executed ? enqued at start and then finish
      // we don't know - we don't want a forever scheduler - but still parallel execution - not waiting for result - but a thread checking the result
      // so thread pool executor wai
      // exception handling - this would let component come through - but was the install succesfull ? maybe not if a failure occurs
      // at least we test faster ...
      //ExecutorService executors = Executors.newSingleThreadExecutor();

      System.out.println("finished enque");


    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }


  @Test
  public void ddlTest()
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

      spanner.getDatabaseAdminClient().dropDatabase(instanceId, databaseId);

      Operation result = spanner.getDatabaseAdminClient().createDatabase(instanceId, databaseId, getStatements())
        .waitFor(RetryOption.totalTimeout(Duration.of(1, ChronoUnit.DAYS)));


      writeReadLargeString(dbClient);

      //readingNullValues(spanner, db);

      System.out.println(result);
      assertTrue(result.isSuccessful());
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private void writeReadLargeString(DatabaseClient dbClient)
  {
    dbClient.write(
      ImmutableList.of(Mutation.newInsertBuilder("test1")
      .set("RowId").to(1)
      .set("LargeString").to(generateLargeString(2621440))
      .build()));

    ResultSet rs = dbClient.singleUse().executeQuery(Statement.of("select largeString from test1 where rowid=1"));
    rs.next();
    String str = rs.getString(0);
    System.out.println(str);
    System.out.println(str.length());
    System.out.println(str.getBytes().length);
  }

  private String generateLargeString(int i)
  {
    StringBuilder sb = new StringBuilder(i);
    for(int y = 0; y < i; y++){
      sb.append("A");
    }
    return sb.toString();
  }

  private void readingNullValues(Spanner spanner, DatabaseId db)
  {
    Mutation mutationInsert = Mutation.newInsertBuilder("test1")
      .set("rowId").to(1)
      //.set("somearray").toStringArray(Arrays.asList("1","2","3"))
      .build();

    spanner.getDatabaseClient(db).write(Arrays.asList(
      mutationInsert
    ));

    ResultSet rs = spanner.getDatabaseClient(db).singleUse().read(
      "test1",
      KeySet.singleKey(Key.of(1)),
      Arrays.asList("rowid", "CUSTID", "somearray"));
    rs.next();


    int i = 1;
    {
      List<String> list = !rs.isNull("") ? rs.getStringList("") : Collections.<String>emptyList();
      String value = i <= list.size() ? list.get(i) : null;
    }


    Double d = null;
    d.floatValue();
    String result = rs.getString("CUSTID"); // java.lang.NullPointerException: Column CUSTID contains NULL value
    //List<String> result = rs.getStringList("somearray"); // java.lang.NullPointerException: Column somearray contains NULL value
    System.out.println(result);
  }

  private List<String> getStatements()
  {
    int v;


    StringBuilder sb = new StringBuilder();
    appender(sb);
    return Arrays.asList(sb.toString().split(";"));
  }

  //sqlcmd.append(";");
  private void appender(StringBuilder sqlcmd)
  {
    ResultSet rs = null;
    sqlcmd.append("CREATE TABLE test1(RowId INT64");
    sqlcmd.append(",LargeString STRING(MAX)");
    sqlcmd.append(") PRIMARY KEY(RowId) ");
  }
}
