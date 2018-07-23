

package com.excellenceengineeringsolutions;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by eXpert on 7/12/2018.
 */
public class TransactionApi
{
  public static void main(String[] args) throws Exception
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
        .setExecutorFactory(new GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService>() {
          private final ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(2);

          @Override
          public ScheduledExecutorService get() {
            return service;
          }

          @Override
          public void release(ScheduledExecutorService service) {
            service.shutdown();
          }
        })
        .build());

    SpannerOptions options = spannerOptionsBuilder.build();

    Spanner spanner = options.getService();
    String instanceId = "eu-instance";
    String databaseId = "vasile2";

    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(db);
    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();


    long singerId = 1;
    try (TransactionManager manager = dbClient.transactionManager()) {
      TransactionContext txn = manager.begin();


      while (true) {
        String column = "FirstName";
        Struct row = txn.readRow("Singers", Key.of(singerId), Collections.singleton(column));
        String name = row.getString(column);
        txn.buffer(
          Mutation.newUpdateBuilder("Singers").set(column).to(name.toUpperCase()).build());
        try {
          manager.commit();
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis() / 1000);
          txn = manager.resetForRetry();
        }
      }
    }
  }
}
