package com.excellenceengineeringsolutions;

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

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ReadThenUpdate extends BaseSpanner
{

  @Test
  public void transaction()
  {
    DatabaseClient dbClient = getDatabaseClient();


        /*dbClient.write(Arrays.asList(
          Mutation.newInsertBuilder("arrayofstring")
            .set("id").to(888)
            .build()));*/

    write(dbClient, Mutation.newUpdateBuilder("arrayofstring")
      .set("id").to(888)
      .set("str").to("asd")
      .set("lng").to((Date) null)
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