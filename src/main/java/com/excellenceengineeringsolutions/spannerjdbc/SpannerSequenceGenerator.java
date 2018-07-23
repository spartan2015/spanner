/* Copyright (c) Nokia Siemens Networks 2007 All Rights Reserved
   The reproduction, transmission or use of this document or its contents
   is not permitted without express written authority. Offenders will be
   liable for damages. All rights, including rights created by patent grant
   or registration of a utility model or design, are reserved.
   Technical modifications possible.
   Technical specifications and features are binding only insofar as they
   are specifically and expressly agreed upon in a written contract.
*/

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider;


import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public final class SpannerSequenceGenerator
{
  public static final String SEQUENCE_TN = "Sequences";
  public static final String SEQUENCE_NAME = "SequenceName";
  static final String SEQUENCE_VALUE = "SequenceValue";
  static final String SEQUENCE_MAX_VALUE = "MaxValue";
  static final String SEQUENCE_MIN_VALUE = "MinValue";
  static final String SEQUENCE_CYCLE = "Cycle";
  static final String SEQUENCE_CACHE = "Cache";
  private static final int DEFAULT_CACHE = 100;
  private static final long DEFAULT_MIN_VALUE = 1L;
  private static final long DEFAULT_MAX_VALUE = Long.MAX_VALUE;
  private static final boolean DEFAULT_CYCLE = false;
  private final ConcurrentMap<String, Queue<Long>> cachedSequenceValues = new ConcurrentHashMap<>();
  private final String tableName;
  private final com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider spannerClientProvider;

  public SpannerSequenceGenerator(SpannerClientProvider spannerClientProvider)
  {
    this.spannerClientProvider = spannerClientProvider;
    this.tableName = SEQUENCE_TN;
  }

  public synchronized void init(final String sequenceName) throws RuntimeException
  {
    init(sequenceName, 1L, 1L, Long.MAX_VALUE, false, 1000);
  }

  public void init(final String sequenceName, final long initialValue,
                   final long minValue, final long maxValue, final boolean cycle, final int cache) throws RuntimeException
  {

    if ( sequenceName == null || sequenceName.trim().isEmpty() )
    {
      throw new RuntimeException("init: incorrect sequenceName is empty");
    }
    if ( minValue > maxValue )
    {
      throw new RuntimeException("init: incorrect minValue: " + minValue + " sequenceName " + sequenceName);
    }
    if ( minValue + cache > maxValue && !cycle )
    {
      throw new RuntimeException("init: incorrect cache: " + cache + " sequenceName " + sequenceName);
    }
    if ( initialValue < minValue || initialValue > maxValue )
    {
      throw new RuntimeException("init: incorrect initialValue: " + initialValue + " sequenceName " + sequenceName);
    }
    Boolean result = spannerClientProvider.getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Boolean>()
    {
      @Override
      public Boolean run(TransactionContext transaction) throws Exception
      {
        try
        {
          Struct row = transaction.readRow(tableName, Key.of(sequenceName),
            java.util.Collections.singletonList(SEQUENCE_VALUE));
          if ( row != null )
          {
            return true; // already inited
          }
          transaction.buffer(Mutation.newInsertOrUpdateBuilder(tableName)
            .set(SEQUENCE_NAME).to(sequenceName)
            .set(SEQUENCE_VALUE).to(initialValue)
            .set(SEQUENCE_MIN_VALUE).to(minValue)
            .set(SEQUENCE_MAX_VALUE).to(maxValue)
            .set(SEQUENCE_CYCLE).to(cycle)
            .set(SEQUENCE_CACHE).to(cache)
            .build());
          return true;
        }
        finally
        {
          transaction.close();
        }
      }
    });
    if ( result == null || !result )
    {
      throw new RuntimeException("init: incorrect sequenceName " + sequenceName);
    }
  }

  public long getNextValue(String sequenceName) throws RuntimeException
  {
    Queue<Long> queue = cachedSequenceValues.get(sequenceName);
    if ( queue == null )
    {
      init(sequenceName);
      cachedSequenceValues.putIfAbsent(sequenceName, new ConcurrentLinkedQueue<Long>());
      queue = cachedSequenceValues.get(sequenceName);
    }
    Long seqValue = queue.poll();
    if ( seqValue == null )
    {
      seqValue = updateSequenceCache(sequenceName);
    }
    return seqValue;
  }

  private Long updateSequenceCache(final String sequenceName) throws RuntimeException
  {
    Queue<Long> queue = cachedSequenceValues.get(sequenceName);
    Long seqValue = queue.poll();
    if ( seqValue != null )
    {
      return seqValue;
    }
    try
    {
      NextValue nextValue = spannerClientProvider.getDatabaseClient()
        .readWriteTransaction()
        .run(new NextValueCallable(tableName, sequenceName));
      if ( nextValue == null )
      {
        throw new RuntimeException("getNextValue is null");
      } else
      {
        for ( long i = nextValue.getCurrentValue(); i < nextValue.getNextValue(); i++ )
        {
          queue.offer(i);
        }
      }
    }
    catch ( SpannerException e )
    {
      if ( e.getCause() instanceof RuntimeException )
      {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(" getNextValue");
    }
    return queue.poll();
  }

  class NextValueCallable implements TransactionCallable<NextValue>
  {
    private final String sequenceName;
    private final String tableName;

    NextValueCallable(String tableName, String sequenceName)
    {
      this.sequenceName = sequenceName;
      this.tableName = tableName;
    }

    @Override
    public NextValue run(TransactionContext transaction) throws Exception
    {
      try
      {
        Struct row = transaction.readRow(
          tableName,
          Key.of(sequenceName),
          Arrays.asList(
            SEQUENCE_VALUE,
            SEQUENCE_MIN_VALUE,
            SEQUENCE_MAX_VALUE,
            SEQUENCE_CACHE,
            SEQUENCE_CYCLE
          )
        );
        long nextValue = 0L;
        long currentValue;
        if ( row == null )
        {
          throw new RuntimeException("Incorrect sequenceName " + sequenceName);
        }
        currentValue = row.getLong(SEQUENCE_VALUE);
        long cache = DEFAULT_CACHE;
        if ( !row.isNull(SEQUENCE_CACHE) )
        {
          cache = row.getLong(SEQUENCE_CACHE);
        }
        long maxValue = DEFAULT_MAX_VALUE;
        if ( !row.isNull(SEQUENCE_MAX_VALUE) )
        {
          maxValue = row.getLong(SEQUENCE_MAX_VALUE);
        }
        if ( (currentValue + cache) > maxValue )
        {
          boolean cycle = DEFAULT_CYCLE;
          if ( !row.isNull(SEQUENCE_CYCLE) )
          {
            cycle = row.getBoolean(SEQUENCE_CYCLE);
          }
          if ( cycle )
          {
            long minValue = DEFAULT_MIN_VALUE;
            if ( !row.isNull(SEQUENCE_MIN_VALUE) )
            {
              minValue = row.getLong(SEQUENCE_MIN_VALUE);
            }
            nextValue = minValue + cache;
            currentValue = minValue;
          } else
          {
            throw new RuntimeException("Incorrect sequenceName " + sequenceName);
          }
        } else
        {
          nextValue = currentValue + cache;
        }
        transaction.buffer(Mutation.newInsertOrUpdateBuilder(tableName)
          .set(SEQUENCE_NAME).to(sequenceName)
          .set(SEQUENCE_VALUE).to(nextValue)
          .build());
        return new NextValue(currentValue, nextValue);
      }
      finally
      {
        transaction.close();
      }
    }
  }

  class NextValue
  {
    private long currentValue;
    private long nextValue;

    NextValue(long currentValue, long nextValue)
    {
      this.currentValue = currentValue;
      this.nextValue = nextValue;
    }

    long getCurrentValue()
    {
      return currentValue;
    }

    long getNextValue()
    {
      return nextValue;
    }
  }
}
