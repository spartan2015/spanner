/*


package com.excellenceengineeringsolutions.spannerjdbc.test;

import com.excellenceengineeringsolutions.db.intf.RuntimeException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementDeleteHandler.spannerDeleteBuilder;
import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementSelectHandler.spannerQueryBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;

*/
/**
 * Test StatementDeleteHandler
 *//*

public class StatementDeleteHandlerIT extends StatementBaseIT
{

  @Test
  public void testParse() throws Exception{
    String deletes = "DELETE FROM PLFBSPAN_BSDRORDERS WHERE OrderID=?\n" +
      "DELETE FROM PLFBSPAN_BSDRORDERS WHERE State=0 AND LASTUPDATETIME<?DELETE FROM PLFBSPAN_BSDRFILES WHERE OrderID=? AND ADDFILETYPE=? AND ADDFILENAME=?";
    String[] statements = deletes.split("\n");
    for(String delete : statements){
      spannerDeleteBuilder(Collections.emptyList(),delete);
    }
  }

  @Test
  public void shouldDeleteOrder() throws Exception
  {
    Long orderId = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      Integer result = spannerDeleteBuilder(Arrays.asList("ORDERID"),
        "delete from plfbSPAN_bsdrorders where" +
        " orderid=?" +
        " and orderversion=?" +
        " and LASTMODIFIEDTIME=?" +
        " and LASTUPDATETIME=200" +
        " and ORDERFILELENGTH=?" +
        " and ORDERFILENAME=?")
        .bind(1, orderId)
        .bind(2, 33L)
        .bind(3, 100L)
        .bind(4, 1L)
        .bind(5, "filename")
        .execute(transaction);
      assertEquals("Must delete", Integer.valueOf(1), result);

      return null;
    });

    assertOrderDeleted(orderId);
  }

  private void assertOrderDeleted(Long orderId) throws RuntimeException
  {
    assertFalse(spannerQueryBuilder("select 1 from plfbSPAN_bsdrorders where ORDERID=?")
      .bind(1, orderId).execute(getClient()).next());
  }

  @Test
  public void shouldDeleteAll() throws Exception
  {
    Long orderId = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      Integer result = spannerDeleteBuilder(Arrays.asList("ORDERID"),
        "delete from plfbSPAN_bsdrorders")
        .execute(transaction);
      assertEquals("Must delete", Integer.valueOf(1), result);

      return null;
    });

    assertOrderDeleted(orderId);
  }
}
*/
