package com.excellenceengineeringsolutions;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class NastyExceptionAfterResultSetNextThatReturnsFalse extends BaseSpanner
{

  @Test
  public void beyondEnd()
  {
    ResultSet rs = getDatabaseClient().singleUse()
      .executeQuery(Statement.of("select 1 from information_schema.tables"));

    while ( rs.next() ) ; // go beyond end
    assertFalse(rs.next());

    try
    {
      assertTrue(rs.isNull(0)); // java.lang.IllegalStateException: ResultSet is closed
      fail();
    }
    catch ( IllegalStateException ex )
    {
      ex.printStackTrace();
    }
  }

  @Test
  public void noNext()
  {
    ResultSet rs = getDatabaseClient().singleUse()
      .executeQuery(Statement.of("select 1 from information_schema.tables"));

    try
    {
      assertTrue(rs.isNull(0)); //java.lang.IllegalStateException: next() call required
      fail();
    }
    catch ( IllegalStateException ex )
    {
      ex.printStackTrace();
    }
  }

  @Test
  public void inReadWriteTransactionNoNext()
  {
    getDatabaseClient().readWriteTransaction().run(
      transaction->{

        ResultSet rs = transaction.executeQuery(Statement.of("select 1 from information_schema.tables"));

        try
        {
          assertTrue(rs.isNull(0)); //java.lang.IllegalStateException: next() call required
          fail();
        }
        catch ( IllegalStateException ex )
        {
          ex.printStackTrace();
        }
        return null;
      }
    );

  }

  @Test
  public void inReadWriteTransactionByondNExt()
  {
    getDatabaseClient().readWriteTransaction().run(
      transaction->{

        ResultSet rs = transaction.executeQuery(Statement.of("select 1 from arrayofstring"));
        while ( rs.next() ) ; // go beyond end
        assertFalse(rs.next());


        try
        {
          assertTrue(rs.isNull(0)); //com.google.cloud.spanner.SpannerException: UNKNOWN: Index: 0, Size: 0
          fail();
        }
        catch ( IllegalStateException ex )
        {
          ex.printStackTrace();
        }
        return null;
      }
    );

  }
}
