

package com.excellenceengineeringsolutions;

import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by eXpert on 6/13/2018.
 */
public class UpdateAll
{
  @Test
  public void updateAll()
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
      String databaseId = "vasile-beyondEnd";
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

      DatabaseClient dbClient = spanner.getDatabaseClient(db);
      DatabaseAdminClient admin = spanner.getDatabaseAdminClient();

    /*  try
      {
        admin.dropDatabase(instanceId, databaseId);
      }
      catch ( Exception ex )
      {
        ex.printStackTrace();
      }
      admin.createDatabase(instanceId, databaseId, Arrays.asList(
        "create table a(a int64, b int64) primary key (a)"
      ));
*/

     /* for ( int i = 0; i < 5; i++ ){

        dbClient.write(Arrays.asList(Mutation.newInsertBuilder("a")
          .set("a").to(i)
          .set("b").to(0)
          .build()));
    }*/

    dbClient.write(Arrays.asList(Mutation.newUpdateBuilder("a")
      .set("a").to(1)
      //.set("b").to(10)
      .build()));

    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }
}
