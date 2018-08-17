//  Source file: P:/advantage/com.optiva.unified.spanner/StatementMain.java

/*
 * Copyright (c) Optiva Inc. 2000 - 2018  All Rights Reserved
 * The reproduction, transmission or use of this document or
 * its contents is not permitted without express written
 * authority. Offenders will be liable for damages. All rights,
 * including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 * Technical modifications possible.
 * Technical specifications and features are binding only
 * insofar as they are specifically and expressly agreed upon
 * in a written contract.
 */

package com.excellenceengineeringsolutions.spannerjdbc;

import com.excellenceengineeringsolutions.AppException;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.excellenceengineeringsolutions.spannerjdbc.SpannerGenericResultSetRead.readFieldAsString;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementDeleteHandler.spannerDeleteBuilder;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementInsertHandler.spannerInsertBuilder;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementSelectHandler.spannerQueryBuilder;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementUpdateHandler.spannerUpdateBuilder;

/**
 */
public class StatementMain
{
  public static final String CN = "StatementMain";
  public static final String CNP = CN + ".";

  public static void main(String[] args) throws Exception
  {
    try
    {
      String projectId = args[0];
      String instanceId = args[1];
      String databaseId = args[2];
      String statement = args[3];

      execute(projectId, instanceId, databaseId, statement);
    }catch(Exception ex){
      System.out.println("Usage parameters: instance db any-query");
      ex.printStackTrace();
    }
  }

  public static void execute(String projectId, String instanceId, String databaseId, String statement) throws AppException
  {
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
    Spanner spanner = null;
    try
    {
      spanner = options.getService();

      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

      DatabaseClient dbClient = spanner.getDatabaseClient(db);



      String upperCase = statement.toUpperCase();
      if ( upperCase.startsWith("INSERT") )
      {
        StatementInsertHandler.InsertMutationHolder insertMutationHolder = spannerInsertBuilder(statement, new HashMap());
        insertMutationHolder.execute(dbClient);
      } else if ( upperCase.startsWith("SELECT") )
      {
        StatementSelectHandler.SelectStatementHolder selectStatementHolder = spannerQueryBuilder(statement);
        ResultSet rs = selectStatementHolder.execute(dbClient);
        boolean headerPrinted = false;
        while ( rs.next() )
        {
          if ( !headerPrinted )
          {
            printHeader(dbClient, rs);
            headerPrinted = true;
          }
          for ( int i = 0; i < rs.getColumnCount(); i++ )
          {
            System.out.print(readFieldAsString(rs, i) + " \t");
          }
          System.out.println();
        }
      } else if ( upperCase.startsWith("DELETE") )
      {
        spannerDeleteBuilder(statement).execute(dbClient);
      } else if ( upperCase.startsWith("UPDATE") )
      {
        spannerUpdateBuilder(statement).execute(dbClient);
      }
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

  private static void printHeader(DatabaseClient dbClient, ResultSet rs)
  {
    for ( int i = 0; i < rs.getColumnCount(); i++ )
    {
      String name = rs.getCurrentRowAsStruct().getType().getStructFields().get(i).getName();
      System.out.println(name + "\t");
    }
  }


}
