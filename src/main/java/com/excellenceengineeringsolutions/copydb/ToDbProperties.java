package com.excellenceengineeringsolutions.copydb;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 *
 */
@Component("ToDbProperties")
@ConfigurationProperties("app")
public class ToDbProperties extends SpannerProperties
{
  public ToDbProperties(
    @Value("${copy.to.project}")
      String project,
    @Value("${copy.to.instance}")
      String instance,
    @Value("${copy.to.database}")
      String database,
    @Value("${copy.to.credentialsFile}")
      String credentialsFile,
    @Value("${spanner.minSessions}")
      int minSessions,
    @Value("${spanner.maxSessions}")
      int maxSessions,
    @Value("${spanner.maxIdleSessions}")
      int maxIdelSessions,
    @Value("${spanner.keepAliveIntervalMinutes}")
      int keepAliveIntervalMinutes,
    @Value("${copy.createNew}")
      boolean createNew,
    @Value("${copy.ignore}")
      String[] ignore
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
    this.createNew = createNew;
    this.ignore = ignore;
  }
}
