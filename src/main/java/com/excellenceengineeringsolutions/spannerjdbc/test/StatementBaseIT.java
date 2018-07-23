/*


package com.excellenceengineeringsolutions.spannerjdbc.test;

import com.excellenceengineeringsolutions.spannerjdbc.StatementDefaults;
import com.excellenceengineeringsolutions.spannerjdbc.StatementInsertHandler;
import com.excellenceengineeringsolutions.spannerjdbc.StatementSelectHandler;
import com.google.cloud.spanner.*;
import com.excellenceengineeringsolutions.spannerjdbc.BitwiseReverser;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerClientProvider;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerSequenceGenerator;
import com.excellenceengineeringsolutions.db.intf.RuntimeException;
import com.excellenceengineeringsolutions.intf.fapi.FrwException;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.readField;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementInsertHandler.spannerInsertBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

*/
/**
 * Test Statement Handlers
 *//*

public class StatementBaseIT
{

  protected StatementDefaults statementDefault;
  private SpannerClientProvider spannerClientProvider;
  private volatile SpannerSequenceGenerator spannerSequenceGenerator;

  public static void abort() throws Exception
  {
    throw new Exception();
  }

  public DatabaseClient getClient()
  {
    return getSpannerClientProvider().getDatabaseClient();
  }

  public SpannerClientProvider getSpannerClientProvider()
  {
    if ( spannerClientProvider == null )
    {
      synchronized ( SpannerClientProvider.class )
      {
        // Double check to minimize contention
        if ( spannerClientProvider != null )
        {
          return spannerClientProvider;
        }
        spannerClientProvider =
          new SpannerClientProvider("grpc:cloudspanner://a-cloud-spanner/eu-instance/vasile?privateKeyPath=" +
            "c:\\dev\\cred-spanner.json&actionOnPoolExhausted=FAIL&maxSessions=1000&minSessions=20" +
            "&writeSessionsFraction=0.5&maxIdleSessions=10&keepAliveIntervalMinutes=1");
      }
    }
    return spannerClientProvider;
  }

  public SpannerSequenceGenerator getSequence() throws RuntimeException
  {
    if ( spannerSequenceGenerator == null )
    {
      synchronized ( SpannerSequenceGenerator.class )
      {
        if ( spannerSequenceGenerator == null )
        {
          spannerSequenceGenerator = new SpannerSequenceGenerator(
            getSpannerClientProvider()
          );
        }
      }
    }
    return spannerSequenceGenerator;
  }

  public long getNextSequenceValue(String sequenceName)
  {
    return BitwiseReverser.reverseBits(getSequence().getNextValue(sequenceName));
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    statementDefault = new StatementDefaults(getSpannerClientProvider(), getSequence());
    cleanOrders();
  }

  private void cleanOrders() throws Exception
  {
    ResultSet rs = getSpannerClientProvider().getDatabaseClient().singleUse()
      .executeQuery(Statement.of("select * from PLFBSPAN_BSDRORDERS"));
    while ( rs.next() )
    {
      getSpannerClientProvider().getDatabaseClient().write(Arrays.asList(
        Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(rs.getLong("ORDERID")))));
    }
  }

  public long insertOrder(TransactionContext ctx) throws Exception
  {
    StatementInsertHandler.InsertMutationHolder insert = spannerInsertBuilder("INSERT INTO PLFBSPAN_BSDRORDERS(ORDERVERSION," +
      "LastModifiedTime,LASTUPDATETIME,OrderFileLength," +
      "State,RESERVATION," +
      "OrderFileName,OrderUserName,ORDERTYPE,ORDERTYPENR," +
      "INTEGRITYSTATE," +
      "ORDERISTEST,FARMSPECIFIC)" +
      "VALUES(33,?,?,?,0,0,?,?,?,?,0,?,?)", statementDefault.getDefaultParameters());
    insert.bind(1, 100);
    insert.bind(2, 200);
    insert.bind(3, 1);
    insert.bind(4, "filename");
    insert.bind(5, "username");
    insert.bind(6, "A");
    insert.bind(7, 1);
    insert.bind(8, "T");
    insert.bind(9, "farm specific");

    Mutation insertMutation = insert.build();
    long id = getGeneratedIdFromMutation(insertMutation);

    ctx.buffer(insertMutation);
    return id;
  }

  private long getGeneratedIdFromMutation(Mutation insertMutation)
  {
    Iterator<String> itColumns = insertMutation.getColumns().iterator();
    Iterator<Value> itValues = insertMutation.getValues().iterator();
    int index = 0;
    Value value = null;
    while ( itColumns.hasNext() )
    {
      String column = itColumns.next();
      value = itValues.next();
      if ( column.equalsIgnoreCase("orderid") )
      {
        break;
      }
    }
    return value.getInt64();
  }

  protected ResultSet getOrder(long id) throws Exception
  {
    ResultSet rs = StatementSelectHandler.spannerQueryBuilder("select * from PLFBSPAN_BSDRORDERS where orderid=?")
      .bind(1, id).execute(getSpannerClientProvider().getDatabaseClient());
    assertTrue(rs.next());
    return rs;
  }

  protected void assertFieldsMatch(ResultSet rs, Map<String, Object> expectedFieldValues)
  {
    for ( String column : expectedFieldValues.keySet() )
    {
      assertEquals(String.format("Field %s does not match",
        column), expectedFieldValues.get(column), readField(rs, column));
    }
  }
}
*/
