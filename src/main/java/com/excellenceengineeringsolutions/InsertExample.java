

package com.excellenceengineeringsolutions;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class InsertExample
{

  @Test
  public void insertTest()
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

    Spanner spanner = options.getService();
    DatabaseId db = DatabaseId.of(projectId, "eu-instance", "vasile");
    DatabaseClient client = spanner.getDatabaseClient(db);



  }
}
