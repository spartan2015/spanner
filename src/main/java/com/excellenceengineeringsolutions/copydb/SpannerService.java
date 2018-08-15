package com.excellenceengineeringsolutions.copydb;

import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class SpannerService
{

  private static final Logger log = LoggerFactory.getLogger(SpannerService.class);

  private SpannerProperties spannerProperties;
  private Spanner spanner;

  public SpannerService(SpannerProperties spannerProperties)
  {
    this.spannerProperties = spannerProperties;
  }

  @PostConstruct
  public void init()
  {
    SpannerOptions spannerOptions = buildSpannerOptions();
    spanner = spannerOptions.getService();
  }

  @PreDestroy
  public void closeSpanner()
  {
    log.info("Closing spanner...");
    try
    {
      spanner.close();
    }
    catch ( IllegalStateException ise )
    {
      //Dirty hack to suppress an error about closing spanner which has already been closed.
      if ( !ise.getMessage()
        .contains("Cloud Spanner client has been closed") )
      {
        throw ise;
      }
    }
    log.info("Closed spanner");
  }

  public Spanner getSpanner()
  {
    return spanner;
  }

  public DatabaseClient getDatabaseClient()
  {

    return spanner
      .getDatabaseClient(DatabaseId.of(InstanceId.of(spannerProperties.getProject(),
        spannerProperties.getInstance()), spannerProperties.getDatabase()));
  }

  public DatabaseAdminClient getDatabaseAdminClient()
  {
    return spanner.getDatabaseAdminClient();
  }

  public List<String> getDdl()
  {
    return spanner.getDatabaseAdminClient()
      .getDatabaseDdl(spannerProperties.getInstance(),spannerProperties.getDatabase());
  }

  private SpannerOptions buildSpannerOptions()
  {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder()
      .setProjectId(spannerProperties.project)
      .setCredentials(buildCredentials(spannerProperties.credentialsFile))
      .setSessionPoolOption(buildSessionPoolOptions());
    builder.setTransportOptions(GrpcTransportOptions.newBuilder()
      .build());
    return builder.build();
  }

  private SessionPoolOptions buildSessionPoolOptions()
  {
    SessionPoolOptions.Builder builder = SessionPoolOptions.newBuilder();
    builder.setMinSessions(spannerProperties.minSessions);
    builder.setMaxIdleSessions(spannerProperties.maxIdleSessions);
    builder.setMaxSessions(spannerProperties.maxSessions);
    builder.setKeepAliveIntervalMinutes(spannerProperties.keepAliveIntervalMinutes);
    return builder.build();
  }

  private ServiceAccountJwtAccessCredentials buildCredentials(String credentialsFile)
  {
    try
    {
      return ServiceAccountJwtAccessCredentials.fromStream(new FileInputStream(credentialsFile));
    }
    catch ( IOException e )
    {
      throw new IllegalArgumentException(
        String.format("Configured spanner credentials file [%s] not found", credentialsFile));
    }
  }

  public SpannerProperties getSpannerProperties()
  {
    return spannerProperties;
  }
}
