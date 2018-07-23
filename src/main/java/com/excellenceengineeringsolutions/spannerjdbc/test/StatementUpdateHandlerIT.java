/*


package com.excellenceengineeringsolutions.spannerjdbc.test;

import com.excellenceengineeringsolutions.RuntimeException;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerMutationStatement;
import com.excellenceengineeringsolutions.spannerjdbc.StatementUpdateHandler;
import org.junit.Test;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


import static com.excellenceengineeringsolutions.spannerjdbc.StatementUpdateHandler.parserUpdate;
import static org.junit.Assert.assertEquals;

*/
/**
 * Test statement update
 *//*

public class StatementUpdateHandlerIT extends StatementBaseIT
{
  public static final String LAST_MODIFIED_TIME = "LastModifiedTime".toUpperCase();
  public static final String ORDER_FILE_LENGTH = "OrderFileLength".toUpperCase();
  public static final String LASTUPDATETIME = "LASTUPDATETIME".toUpperCase();

  @Test
  public void shouldUpdateOrder() throws Exception
  {
    long firstOrder = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    long secondOrder = getClient().readWriteTransaction().run(transaction ->
    {
      return insertOrder(transaction);
    });

    long lastModifiedTime = 777;
    long orderFileLength = 333;
    long lastUpdateTime = 888;

    getClient().readWriteTransaction().run(transaction ->
    {
      int paramIndex = 1;
      SpannerMutationStatement statement = StatementUpdateHandler.spannerUpdateBuilder(Arrays.asList("orderId"),
        "UPDATE PLFBSPAN_BSDRORDERS SET " +
          "LastModifiedTime=?," +
          "OrderFileLength=?," +
          "LASTUPDATETIME=? " +
          "WHERE OrderID=?")
        .bind(paramIndex++, lastModifiedTime)
        .bind(paramIndex++, orderFileLength)
        .bind(paramIndex++, lastUpdateTime)
        .bind(paramIndex++, secondOrder);

      statement.execute(transaction);

      int rowsAffected = ((StatementUpdateHandler.UpdateMutationHolder) statement).getChangeCount();

      assertEquals("Rows affected", 1, rowsAffected);
      return null;
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      Map expectedFieldValues = new HashMap();
      expectedFieldValues.put(LAST_MODIFIED_TIME, lastModifiedTime);
      expectedFieldValues.put(ORDER_FILE_LENGTH, orderFileLength);
      expectedFieldValues.put(LASTUPDATETIME, lastUpdateTime);
      assertFieldsMatch(getOrder(secondOrder), expectedFieldValues);
      assertFieldsMatch(getOrder(firstOrder), defaultOrderInsertValues);
      return null;
    });


  }

  @Test
  public void shouldParseUpdateStatements() throws RuntimeException
  {
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LastModifiedTime=?,OrderFileLength=?,LASTUPDATETIME=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LastModifiedTime=?,OrderFileLength=?,LASTUPDATETIME=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=1 WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=1", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,RESERVATION=1,FARMNAME=?,CEID=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,RESERVATION=1,FARMNAME=?,CEID=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRCONFIG SET LOCKTIME=?, FARMNAME=? WHERE LOCKNAME='polling' AND CEID=?");
      assertEquals("PLFBSPAN_BSDRCONFIG", parts[0]);
      assertEquals("LOCKTIME=?, FARMNAME=?", parts[1]);
      assertEquals("LOCKNAME='polling' AND CEID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRCONFIG SET LOCKTIME=?,FARMNAME=?,CEID=?,FILERID=? WHERE LOCKNAME='polling'");
      assertEquals("PLFBSPAN_BSDRCONFIG", parts[0]);
      assertEquals("LOCKTIME=?,FARMNAME=?,CEID=?,FILERID=?", parts[1]);
      assertEquals("LOCKNAME='polling'", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRCONFIG SET LOCKVALUE=?,LOCKTIME=? WHERE LOCKNAME='bfsmAddress'");
      assertEquals("PLFBSPAN_BSDRCONFIG", parts[0]);
      assertEquals("LOCKVALUE=?,LOCKTIME=?", parts[1]);
      assertEquals("LOCKNAME='bfsmAddress'", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRGAP SET STARTTIME=?,ENDTIME=?,LASTUPDATETIME=?,TYPE=? WHERE ORDERUSERNAME=? AND ORDERTYPE=? AND CLUSTERNAME=?");
      assertEquals("PLFBSPAN_BSDRGAP", parts[0]);
      assertEquals("STARTTIME=?,ENDTIME=?,LASTUPDATETIME=?,TYPE=?", parts[1]);
      assertEquals("ORDERUSERNAME=? AND ORDERTYPE=? AND CLUSTERNAME=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,INTEGRITYSTATE=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,INTEGRITYSTATE=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3 WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRPERIOD SET LASTEXECUTIONSTATE=?,LASTEXECUTIONTIME=?,ORDERLASTDATE=?,ORDERPERIOD=?,NUMBEROFEXECUTIONS=?,SCHEDULESTARTTIME=?,TIMESOFDAY=?,DAYSOFWEEK=?,DAYSOFMONTH=?,PUBLICHOLIDAYS=?,BANKHOLIDAYS=?,ADDITIONALEXECUTIONTIMES=?,EXECUTIONCOUNTER=?,IMMEDIATEEXECUTIONTIME=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRPERIOD", parts[0]);
      assertEquals("LASTEXECUTIONSTATE=?,LASTEXECUTIONTIME=?,ORDERLASTDATE=?,ORDERPERIOD=?,NUMBEROFEXECUTIONS=?,SCHEDULESTARTTIME=?,TIMESOFDAY=?,DAYSOFWEEK=?,DAYSOFMONTH=?,PUBLICHOLIDAYS=?,BANKHOLIDAYS=?,ADDITIONALEXECUTIONTIMES=?,EXECUTIONCOUNTER=?,IMMEDIATEEXECUTIONTIME=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET RESERVATION=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("RESERVATION=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3 WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("LASTUPDATETIME=?,State=?,RESERVATION=?,ORDERSTARTTIME=?,ORDERCONTEXTID=?,CONTEXTIDASYNCH=?,ORDERISPERIODIC=?,CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=? WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACUSER=?,MACVALUE=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=?", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3 WHERE OrderID=?");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("CURRENTCOMMANDNB=?,CURRENTRECNBOFCURRENTCOMMANDS=?,CURRENTCOMMANDPROGRESS=?,TOTALRECNBOFCURRENTCOMMAND=?,TOTALNBOFCOMMANDS=?,CURRENTCOMMANDNAME=?,MACDATE=?,MACORIGIN=?,MACSEQUENCEID=?,INTEGRITYSTATE=3", parts[1]);
      assertEquals("OrderID=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRFILES SET INTEGRITYSTATE=? WHERE OrderID=? AND ADDFILETYPE=? AND ADDFILENAME=?");
      assertEquals("PLFBSPAN_BSDRFILES", parts[0]);
      assertEquals("INTEGRITYSTATE=?", parts[1]);
      assertEquals("OrderID=? AND ADDFILETYPE=? AND ADDFILENAME=?", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET ORDERISCANCELED=1 WHERE OrderID=? AND State IN(0,1,2,3,4,8,9,12,15,16,18,14)");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("ORDERISCANCELED=1", parts[1]);
      assertEquals("OrderID=? AND State IN(0,1,2,3,4,8,9,12,15,16,18,14)", parts[2]);
    }
    {
      String[] parts = parserUpdate("UPDATE PLFBSPAN_BSDRORDERS SET ORDERISCANCELED=1 WHERE OrderUserName=? AND OrderFileName=? AND LastModifiedTime=? AND State IN(0,1,2,3,4,8,9,12,15,16,18,14)");
      assertEquals("PLFBSPAN_BSDRORDERS", parts[0]);
      assertEquals("ORDERISCANCELED=1", parts[1]);
      assertEquals("OrderUserName=? AND OrderFileName=? AND LastModifiedTime=? AND State IN(0,1,2,3,4,8,9,12,15,16,18,14)", parts[2]);
    }
  }

}
*/
