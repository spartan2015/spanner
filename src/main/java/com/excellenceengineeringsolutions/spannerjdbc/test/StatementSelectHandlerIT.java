/*


package com.excellenceengineeringsolutions.spannerjdbc.test;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementHandlerCommon.spannerQueryParams;
import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementInsertHandlerIT.defaultOrderInsertValues;
import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementSelectHandler.spannerQueryBuilder;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

*/
/**
 * Test StatementSelectHandler
 *//*

public class StatementSelectHandlerIT extends StatementBaseIT
{

  @Test
  public void testParse(){
    assertEquals("select * from a where c1=@_1 and c2=@_2", spannerQueryParams("select * from a where c1=? and c2=?", new AtomicInteger(1)));
  }

  @Test
  public void shouldSelectOrder() throws Exception
  {
    Long orderId = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRORDERS where" +
        " ORDERID=?" +
        " and orderversion=?" +
        " and LASTMODIFIEDTIME=?" +
        " and LASTUPDATETIME=200" +
        " and ORDERFILELENGTH=?" +
        " and ORDERFILENAME=?" +
        " order by orderId" +
        " limit 1")
        .bind(1, orderId)
        .bind(2, 33L)
        .bind(3, 100L)
        .bind(4, 1L)
        .bind(5, "filename")
        .execute(transaction);
      assertTrue(rs.next());

      assertFieldsMatch(rs, defaultOrderInsertValues);

      transaction.buffer(
        Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(rs.getLong("ORDERID"))));
      return null;
    });
  }

  @Test
  public void shouldSelectAll() throws Exception
  {
    Long orderId = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRORDERS")
        .execute(transaction);
      assertTrue(rs.next());

      assertFieldsMatch(rs, defaultOrderInsertValues);

      transaction.buffer(
        Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(rs.getLong("ORDERID"))));
      return null;
    });
  }
}
*/
