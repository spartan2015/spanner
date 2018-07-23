

package com.excellenceengineeringsolutions;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by eXpert on 6/5/2018.
 */
public class BatchApi
{
  /*
  public void t()
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

    int numThreads = Runtime.getRuntime().availableProcessors();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    BatchClient batchClient = spanner.getBatchClient(
      DatabaseId.of(options.getProjectId(), instanceId, databaseId));

    final BatchReadOnlyTransaction txn =
      batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    // A Partition object is serializable and can be used from a different process.
    List<Partition> partitions = txn.partitionQuery(PartitionOptions.getDefaultInstance(),
      Statement.of("SELECT * FROM Sequences"));


    for ( final Partition p : partitions )
    {
      executor.execute(() ->
      {
        try ( ResultSet results = txn.execute(p) )
        {
          while ( results.next() )
          {
            long singerId = results.getLong(0);
            String firstName = results.getString(1);
            String lastName = results.getString(2);
            System.out.println("[" + singerId + "] " + firstName + " " + lastName);
            //  totalRecords.getAndIncrement();
          }
        }
      });
    }
    executor.shutdown();
  }finally{
        try
        {
          executor.awaitTermination(1, TimeUnit.HOURS);
        }
        catch ( InterruptedException e )
        {
          e.printStackTrace();
        }
      }
  }
  */
}
