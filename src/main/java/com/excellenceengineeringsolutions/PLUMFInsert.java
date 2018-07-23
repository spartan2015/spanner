

package com.excellenceengineeringsolutions;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.xml.transform.Result;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class PLUMFInsert
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

    // rel 1 PLFUMF_FUNCTIONS to PLFUMF_FUNCTION_TO_BO_TYPES
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_FUNCTIONS")
        .set("NAME").to("n1")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("VERSION").to(1)
        .build()));


    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_FUNCTION_TO_BO_TYPES")
        .set("NAME").to("n1")
        .set("BO_TYPE").to("n1")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_FUNCTIONS", Key.of("n1"))));


    // rel 2 PLFUMF_FUNCTIONS to PLFUMF_ROLE_PERFORM_FUNCTIONS
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_FUNCTIONS")
        .set("NAME").to("n1")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_ROLE_PERFORM_FUNCTIONS")
        .set("NAME").to("n1")
        .set("ROLE_NAME").to("n1")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_FUNCTIONS", Key.of("n1"))));


    // rel 3 PLFUMF_USERS PLFUMF_USER_TO_CLUSTER
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USERS")
        .set("INTERNAL_ID").to(1)
        .set("NAME").to("User1")
        .set("PARENT_CUSTOMER").to(1)
        .set("BO_TYPE").to("BO_TYPE")
        .set("LOCK_LEVEL").to(1)
        .set("IS_SUPERUSER").to("Y")
        .set("IS_MACHINE_USER").to("Y")
        .set("IS_TICKET_USER").to("Y")
        .set("IS_BATCH_USER").to("Y")
        .set("IS_TECHNICAL_USER").to("Y")
        .set("IS_EXTERNAL_AUTH").to("Y")
        .set("PASSWORD_CHANGE_REQUESTED").to("N")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_COUNT").to(1)
        .set("FAULTY_LOGIN_TIME1").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME2").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME3").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME4").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME5").to(Timestamp.of(new Date()))
        .set("LAST_SUCCESSFUL_LOGIN_TIME").to(Timestamp.of(new Date()))
        .set("LAST_PASSWORD_CHANGE_TIME").to(Timestamp.of(new Date()))
        .set("EXPIRATION_TIME").to(Timestamp.of(new Date()))
        .set("IS_TICKET_IMPORT_USER").to("Y")
        .set("HAS_KEY_SET").to("Y")
        .set("INTEGRITY_PROTECTION").to(1)
        .set("LOCK_FLAGS").to(0)
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USER_TO_CLUSTER")
        .set("INTERNAL_ID").to(1)
        .set("NODE_ID").to("n1")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_USERS", Key.of(1))));


    // PLFUMF_USERS PLFUMF_EXTERNAL_IDS
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USERS")
        .set("INTERNAL_ID").to(1)
        .set("NAME").to("User1")
        .set("PARENT_CUSTOMER").to(1)
        .set("BO_TYPE").to("BO_TYPE")
        .set("LOCK_LEVEL").to(1)
        .set("IS_SUPERUSER").to("Y")
        .set("IS_MACHINE_USER").to("Y")
        .set("IS_TICKET_USER").to("Y")
        .set("IS_BATCH_USER").to("Y")
        .set("IS_TECHNICAL_USER").to("Y")
        .set("IS_EXTERNAL_AUTH").to("Y")
        .set("PASSWORD_CHANGE_REQUESTED").to("N")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_COUNT").to(1)
        .set("FAULTY_LOGIN_TIME1").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME2").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME3").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME4").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME5").to(Timestamp.of(new Date()))
        .set("LAST_SUCCESSFUL_LOGIN_TIME").to(Timestamp.of(new Date()))
        .set("LAST_PASSWORD_CHANGE_TIME").to(Timestamp.of(new Date()))
        .set("EXPIRATION_TIME").to(Timestamp.of(new Date()))
        .set("IS_TICKET_IMPORT_USER").to("Y")
        .set("HAS_KEY_SET").to("Y")
        .set("INTEGRITY_PROTECTION").to(1)
        .set("LOCK_FLAGS").to(0)
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_EXTERNAL_IDS")
        .set("INTERNAL_ID").to(1)
        .set("EXTERNAL_ID").to("e1")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_USERS", Key.of(1))));

    // PLFUMF_USERS PLFUMF_USER_TO_ROLES
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USERS")
        .set("INTERNAL_ID").to(1)
        .set("NAME").to("User1")
        .set("PARENT_CUSTOMER").to(1)
        .set("BO_TYPE").to("BO_TYPE")
        .set("LOCK_LEVEL").to(1)
        .set("IS_SUPERUSER").to("Y")
        .set("IS_MACHINE_USER").to("Y")
        .set("IS_TICKET_USER").to("Y")
        .set("IS_BATCH_USER").to("Y")
        .set("IS_TECHNICAL_USER").to("Y")
        .set("IS_EXTERNAL_AUTH").to("Y")
        .set("PASSWORD_CHANGE_REQUESTED").to("N")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_COUNT").to(1)
        .set("FAULTY_LOGIN_TIME1").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME2").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME3").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME4").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME5").to(Timestamp.of(new Date()))
        .set("LAST_SUCCESSFUL_LOGIN_TIME").to(Timestamp.of(new Date()))
        .set("LAST_PASSWORD_CHANGE_TIME").to(Timestamp.of(new Date()))
        .set("EXPIRATION_TIME").to(Timestamp.of(new Date()))
        .set("IS_TICKET_IMPORT_USER").to("Y")
        .set("HAS_KEY_SET").to("Y")
        .set("INTEGRITY_PROTECTION").to(1)
        .set("LOCK_FLAGS").to(0)
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USER_TO_ROLES")
        .set("INTERNAL_ID").to(1)
        .set("ROLE_NAME").to("r1")
        .set("SMAF_FLAG").to("Y")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_USERS", Key.of(1))));

    // PLFUMF_USERS PLFUMF_PASSWORD_HISTORY
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USERS")
        .set("INTERNAL_ID").to(1)
        .set("NAME").to("User1")
        .set("PARENT_CUSTOMER").to(1)
        .set("BO_TYPE").to("BO_TYPE")
        .set("LOCK_LEVEL").to(1)
        .set("IS_SUPERUSER").to("Y")
        .set("IS_MACHINE_USER").to("Y")
        .set("IS_TICKET_USER").to("Y")
        .set("IS_BATCH_USER").to("Y")
        .set("IS_TECHNICAL_USER").to("Y")
        .set("IS_EXTERNAL_AUTH").to("Y")
        .set("PASSWORD_CHANGE_REQUESTED").to("N")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_COUNT").to(1)
        .set("FAULTY_LOGIN_TIME1").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME2").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME3").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME4").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME5").to(Timestamp.of(new Date()))
        .set("LAST_SUCCESSFUL_LOGIN_TIME").to(Timestamp.of(new Date()))
        .set("LAST_PASSWORD_CHANGE_TIME").to(Timestamp.of(new Date()))
        .set("EXPIRATION_TIME").to(Timestamp.of(new Date()))
        .set("IS_TICKET_IMPORT_USER").to("Y")
        .set("HAS_KEY_SET").to("Y")
        .set("INTEGRITY_PROTECTION").to(1)
        .set("LOCK_FLAGS").to(0)
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_PASSWORD_HISTORY")
        .set("INTERNAL_ID").to(1)
        .set("PASSWORD").to(ByteArray.copyFrom("a"))
        .set("TIMESTAMP").to(Timestamp.of(new Date()))
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_USERS", Key.of(1))));


    // PLFUMF_USERS PLFUMF_USER_PROPERTIES
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USERS")
        .set("INTERNAL_ID").to(1)
        .set("NAME").to("User1")
        .set("PARENT_CUSTOMER").to(1)
        .set("BO_TYPE").to("BO_TYPE")
        .set("LOCK_LEVEL").to(1)
        .set("IS_SUPERUSER").to("Y")
        .set("IS_MACHINE_USER").to("Y")
        .set("IS_TICKET_USER").to("Y")
        .set("IS_BATCH_USER").to("Y")
        .set("IS_TECHNICAL_USER").to("Y")
        .set("IS_EXTERNAL_AUTH").to("Y")
        .set("PASSWORD_CHANGE_REQUESTED").to("N")
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_COUNT").to(1)
        .set("FAULTY_LOGIN_TIME1").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME2").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME3").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME4").to(Timestamp.of(new Date()))
        .set("FAULTY_LOGIN_TIME5").to(Timestamp.of(new Date()))
        .set("LAST_SUCCESSFUL_LOGIN_TIME").to(Timestamp.of(new Date()))
        .set("LAST_PASSWORD_CHANGE_TIME").to(Timestamp.of(new Date()))
        .set("EXPIRATION_TIME").to(Timestamp.of(new Date()))
        .set("IS_TICKET_IMPORT_USER").to("Y")
        .set("HAS_KEY_SET").to("Y")
        .set("INTEGRITY_PROTECTION").to(1)
        .set("LOCK_FLAGS").to(0)
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_USER_PROPERTIES")
        .set("INTERNAL_ID").to(1)
        .set("PROPERTY_NAME").to("a")
        .set("STRING_VALUE1").to("v")
        .set("VERSION").to(1)
        .set("SEQ").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_USERS", Key.of(1))));


    client.write(Collections.singletonList(Mutation.delete("PLFUMF_ROLES", Key.of("n1"))));

    // PLFUMF_ROLES PLFUMF_ROLE_MAY_ASSIGN_ROLES
    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_ROLES")
        .set("NAME").to("n1")
        .set("BO_TYPE").to("BO_TYPE")
        .set("PARENT_CUSTOMER").to(1)
        .set("CREATION_TIME").to(Timestamp.of(new Date()))
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.newInsertBuilder("PLFUMF_ROLE_MAY_ASSIGN_ROLES")
        .set("NAME").to("n1")
        .set("MAY_ASSIGN").to("a")
        .set("VERSION").to(1)
        .build()));

    client.write(Collections.singletonList(Mutation.delete("PLFUMF_ROLES", Key.of("n1"))));


  }





}
