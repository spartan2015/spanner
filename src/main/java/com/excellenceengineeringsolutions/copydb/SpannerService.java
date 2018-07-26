package com.excellenceengineeringsolutions.copydb;

import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

@Service
public class SpannerService {

    private static final Logger log = LoggerFactory.getLogger(SpannerService.class);

    private final String projectId;
    private final int minSessions;
    private final int maxIdleSessions;
    private final int maxSessions;
    private final int keepAliveIntervalMinutes;
    private final Resource authKey;

    private final Spanner spanner;

    SpannerService(
            @Value("${spanner.projectId}") String projectId,
            @Value("${spanner.minSessions}") int minSessions,
            @Value("${spanner.maxIdleSessions}") int maxIdleSessions,
            @Value("${spanner.maxSessions}") int maxSessions,
            @Value("${spanner.keepAliveIntervalMinutes}") int keepAliveIntervalMinutes,
            @Value("classpath:creds.json") Resource authKey) {
        this.projectId = projectId;
        this.minSessions = minSessions;
        this.maxIdleSessions = maxIdleSessions;
        this.maxSessions = maxSessions;
        this.keepAliveIntervalMinutes = keepAliveIntervalMinutes;
        this.authKey = authKey;

        SpannerOptions spannerOptions = buildSpannerOptions();
        spanner = spannerOptions.getService();
    }

    DatabaseClient getDatabaseClient(String instanceId, String databaseId) {
        return spanner.getDatabaseClient(DatabaseId.of(InstanceId.of(projectId, instanceId), databaseId));
    }

    List<String> getDdl(String instanceId, String databaseId) {
        return spanner.getDatabaseAdminClient().getDatabaseDdl(instanceId, databaseId);
    }

    private SpannerOptions buildSpannerOptions() {
        SpannerOptions.Builder builder = SpannerOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(buildCredentials())
                .setSessionPoolOption(buildSessionPoolOptions());
        builder.setTransportOptions(GrpcTransportOptions.newBuilder().build());
        return builder.build();
    }

    private SessionPoolOptions buildSessionPoolOptions() {
        SessionPoolOptions.Builder builder = SessionPoolOptions.newBuilder();
        builder.setMinSessions(minSessions);
        builder.setMaxIdleSessions(maxIdleSessions);
        builder.setMaxSessions(maxSessions);
        builder.setKeepAliveIntervalMinutes(keepAliveIntervalMinutes);
        return builder.build();
    }

    private ServiceAccountJwtAccessCredentials buildCredentials() {
        try {
            return ServiceAccountJwtAccessCredentials.fromStream(new FileInputStream(authKey.getFile()));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Configured spanner credentials file [%s] not found", authKey));
        }
    }
}
