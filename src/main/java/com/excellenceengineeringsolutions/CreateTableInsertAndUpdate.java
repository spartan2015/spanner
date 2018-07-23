

package com.excellenceengineeringsolutions;

import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

public class CreateTableInsertAndUpdate
{

  @Test
  public void insertTest() {
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
    adminClient.createDatabase(instanceId,databaseId,Arrays.asList("create table t1(rowid int64, value string(10)) primary key (rowid)"))
      .waitFor();

    String initialValue = "123";
    client.write(Arrays.asList(Mutation.newInsertBuilder("t1")
            .set("rowid").to(1)
            .set("value").to(initialValue)
            .build()));

    assertValue(client, initialValue);

    String newValue = "1234";
    client.write(Arrays.asList(Mutation.newUpdateBuilder("t1")
            .set("rowid").to("2")
            .set("value").to(newValue)
            .build()));



    assertValue(client, newValue);
  }

  private void assertValue(DatabaseClient client, String initialValue) {
    ResultSet resultSet = client.singleUse().read("t1", KeySet.singleKey(Key.of(1)), Arrays.asList("value"));
    resultSet.next();
    assertEquals(initialValue, resultSet.getString(0));
  }
}
