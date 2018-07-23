

package com.excellenceengineeringsolutions;

import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * works
 */
public class SetNull
{

  @Test
  public void t(){
    assertNull(new File("/asd").listFiles());
  }

  @Test
  public void testSetNull() {
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
    DatabaseClient client = spanner.getDatabaseClient(db);
    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();

    adminClient.dropDatabase(instanceId,databaseId);
    adminClient.createDatabase(instanceId,databaseId,Arrays.asList("create table testSetNull(rowid int64, value string(10)) primary key (rowid)"))
      .waitFor();

    String initialValue = "123";
    client.write(Arrays.asList(Mutation.newInsertBuilder("testSetNull")
            .set("rowid").to(1)
            .set("value").to((String)null)
            .build()));

  }


}
