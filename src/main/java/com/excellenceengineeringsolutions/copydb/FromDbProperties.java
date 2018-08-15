package com.excellenceengineeringsolutions.copydb;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 *
 */
@Component("FromDbProperties")
@ConfigurationProperties("app")
public class FromDbProperties extends SpannerProperties
{
  public FromDbProperties(
    @Value("${copy.from.project}")
      String project,
    @Value("${copy.from.instance}")
      String instance,
    @Value("${copy.from.database}")
    String database,
    @Value("${copy.from.credentialsFile}")
      String credentialsFile,
    @Value("${spanner.minSessions}")
      int minSessions,
    @Value("${spanner.maxSessions}")
      int maxSessions,
    @Value("${spanner.maxIdleSessions}")
      int maxIdelSessions,
    @Value("${spanner.keepAliveIntervalMinutes}")
      int keepAliveIntervalMinutes
  )
  {
    this.project = project;
    this.instance = instance;
    this.database = database;
    this.credentialsFile = credentialsFile;
    this.minSessions = minSessions;
    this.maxSessions = maxSessions;
    this.maxIdleSessions = maxIdelSessions;
    this.keepAliveIntervalMinutes = keepAliveIntervalMinutes;
  }
}
