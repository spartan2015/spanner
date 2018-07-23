

package com.excellenceengineeringsolutions;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import com.google.common.io.BaseEncoding;
import org.junit.Test;

import javax.sound.midi.Soundbank;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class ReadATFRT
{

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
      String instanceId = "cloud-instance-2";
      String databaseId = "custdb";
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      String ratePlanId = "1-200_005_0043";
      String[] tables = getTables(dbClient,"ATFRT");

      List<String> tablesWithBoth = new ArrayList<>();
      List<String> tablesWithCUSTID = new ArrayList<>();
      List<String> tablesWithParentId = new ArrayList<>();

      long count = 0;
      for(String table : tables) {
        boolean hasCustomerId = hasColumn(dbClient, table, "CUSTID");
        boolean hasParentId = hasColumn(dbClient, table, "ParentId");

        Statement statement = null;
        if (hasCustomerId && hasParentId){
          statement =
                  Statement.newBuilder(String.format("select count(*) from %s " +
                  "where CUSTID=@value or ParentId = @value",table))
                  .bind("value").to(ratePlanId)
                  .build();
        }else if (hasCustomerId){
          statement =
                  Statement.newBuilder(String.format("select count(*) from %s " +
                          "where CUSTID=@value",table))
                          .bind("value").to(ratePlanId)
                          .build();
        }else if (hasParentId){
          statement =
                  Statement.newBuilder(String.format("select count(*) from %s " +
                          "where ParentId = @value",table))
                          .bind("value").to(ratePlanId)
                          .build();
        }
        ResultSet resultSet = dbClient.singleUse()
                .executeQuery(statement);
        resultSet.next();

        long found = resultSet.getLong(1);
        count+=found;
        System.out.println("count: " + found);
      }
      System.out.println("Tables with key: " + count);
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private String[] getTables(DatabaseClient client, String tablePrefix) {
    List<String> result = new ArrayList();
    ResultSet resultSet = client.singleUse().executeQuery(
            Statement.newBuilder("select table_name from Information_schema.tables where table_name like @tablePrefix")
                    .bind("tablePrefix").to(tablePrefix)
                    .build());
    while(resultSet.next()){
     result.add(resultSet.getString(0));
    }
    return result.toArray(new String[result.size()]);
  }

  public boolean hasColumn(DatabaseClient client, String tableName, String columnName){
    ResultSet resultSet = client.singleUse().executeQuery(
            Statement.newBuilder("select true from Information_schema.columns " +
                    "where table_name=@tableName and column_name=@columnName")
                    .bind("tableName").to(tableName)
                    .bind("columnName").to( columnName)
                    .build());
    return resultSet.next();
  }
}
