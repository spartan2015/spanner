

package com.excellenceengineeringsolutions;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class PLFBATTest
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

    // rel 1 PLFBSPAN_BSDRORDERS  to PLFBSPAN_BSDRFILES
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFBSPAN_BSDRORDERS")
        .set("ORDERID").to(1)
        .set("ORDERVERSION").to(1)
        .set("LASTMODIFIEDTIME").to(1)
        .set("LASTUPDATETIME").to(1)
        .set("ORDERFILELENGTH").to(1)
        .set("STATE").to(1)
        .set("RESERVATION").to(1)
        .set("ORDERFILENAME").to("1")
        .set("ORDERUSERNAME").to("1")
        .set("ORDERTYPE").to("1")
        .set("ORDERTYPENR").to(1)
        .set("ORDERSTARTTIME").to(1)
        .set("ORDERCONTEXTID").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFBSPAN_BSDRFILES")
        .set("ORDERID").to(1)
        .set("ADDFILETYPE").to(1)
        .set("ADDFILENAME").to("1")
        .set("SEQ").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(1))));

    // rel 2 PLFBSPAN_BSDRORDERS  to PLFBSPAN_BSDRPERIOD
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFBSPAN_BSDRORDERS")
        .set("ORDERID").to(1)
        .set("ORDERVERSION").to(1)
        .set("LASTMODIFIEDTIME").to(1)
        .set("LASTUPDATETIME").to(1)
        .set("ORDERFILELENGTH").to(1)
        .set("STATE").to(1)
        .set("RESERVATION").to(1)
        .set("ORDERFILENAME").to("1")
        .set("ORDERUSERNAME").to("1")
        .set("ORDERTYPE").to("1")
        .set("ORDERTYPENR").to(1)
        .set("ORDERSTARTTIME").to(1)
        .set("ORDERCONTEXTID").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFBSPAN_BSDRPERIOD")
        .set("ORDERID").to(1)
        .set("ORDERPERIOD").to(1)
        .set("ORDERLASTDATE").to(1)
        .set("LASTEXECUTIONTIME").to(1)
        .set("LASTEXECUTIONSTATE").to(1)
        .set("SEQ").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(1))));
  }
}
