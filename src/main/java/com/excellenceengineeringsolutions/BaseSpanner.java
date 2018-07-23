package com.excellenceengineeringsolutions;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 *
 */
public class BaseSpanner
{
  private DatabaseClient dbClient;
  private Spanner spanner;
  public static final String INSTANCE_ID = "eu-instance";
  public static final String DATABASE_ID = "vasile";
  public static final String PROJECT_ID = "redknee-cloud-spanner";

  public DatabaseClient getDatabaseClient()
  {
    if ( dbClient == null )
    {
      SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder()
        .setProjectId(PROJECT_ID)
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
      spanner = options.getService();

      DatabaseId db = DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID);

      dbClient = spanner.getDatabaseClient(db);
    }
    return dbClient;
  }
}
