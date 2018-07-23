

package com.excellenceengineeringsolutions;

import com.google.cloud.RetryOption;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import org.junit.Test;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;


public class ReadCloud
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

      System.out.println("fu");
      read(dbClient);
      System.out.println("su");
      readSu(dbClient);
      System.out.println("fu by select");
      readMsisdn(dbClient,"");
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private void read(DatabaseClient dbClient)
  {
    ResultSet resultSet = dbClient.singleUse().read("SPAN_FHY_TEST", KeySet.singleKey(Key.of(132001)), Arrays.asList("fu_container"));
    if (resultSet.next())
    {
      printContainerData(resultSet);
    }
  }

  private void readSu(DatabaseClient dbClient)
  {
    ResultSet resultSet = dbClient.singleUse().read("SPAN_SHY_TEST", KeySet.singleKey(Key.of(132001)), Arrays.asList("su_container"));
    if (resultSet.next())
    {
      printContainerData(resultSet);
    }
  }

  private void printContainerData(ResultSet resultSet)
  {
    byte[] containerData = resultSet.getBytes(0).toByteArray();
    String base64 = BaseEncoding.base16().encode(containerData);
    System.out.println(base64);
  }

  private void readMsisdn(DatabaseClient dbClient, String msisdn){


    String select = "SELECT fu_container FROM SPAN_ACC_TEST a inner join SPAN_ENT_TEST e on a.ENTITY_DEF_ID=e.ENTITY_DEF_ID and a.ENTITY_PK_ATTRS=e.ENTITY_PK_ATTRS inner join SPAN_FHY_TEST s on e.container_id= s.container_id and e.partition_id=s.partition_id     " +
      "WHERE a.ENTITY_PK_ATTRS LIKE '491234567892%';";

    ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of(select));

    if (resultSet.next())
    {
      printContainerData(resultSet);
    }

  }


}
