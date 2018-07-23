

package com.excellenceengineeringsolutions;

import com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ReadThenUpdate {

    @Test
    public void transaction(){
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

        String instanceId = "eu-instance";
        String databaseId = "vasile";
        DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

        DatabaseClient dbClient = spanner.getDatabaseClient(db);

        /*dbClient.write(Arrays.asList(
          Mutation.newInsertBuilder("arrayofstring")
            .set("id").to(888)
            .build()));*/

        write(dbClient,  Mutation.newUpdateBuilder("arrayofstring")
          .set("id").to(888)
          .set("str").to("asd")
          .set("lng").to((Date)null)
          .build());

        ResultSet rs = dbClient.singleUse().executeQuery(
          Statement.of("select * from arrayofstring where id = 888")
        );

        rs.next();
        System.out.println(rs.isNull("str"));
        System.out.println(rs.getColumnType("str"));
        System.out.println("value: " + rs.getString("str"));

        System.out.println("value: " + rs.getLong("lng"));

    }

    private Timestamp write(DatabaseClient dbClient, Mutation update)
    {
        return dbClient.write(Arrays.asList(
          update
        ));
    }

}