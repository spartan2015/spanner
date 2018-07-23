/*


package com.excellenceengineeringsolutions.spannerjdbc.test;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Value;
import com.excellenceengineeringsolutions.db.intf.RuntimeException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementInsertHandler.*;
import static de.siemens.advantage.platform.batch.batch.impl.spanner.StatementSelectHandler.spannerQueryBuilder;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

*/
/**
 * Test StatementHandlerCommon class
 *//*

public class StatementInsertHandlerIT extends StatementBaseIT
{
  protected static Map<String, Object> defaultOrderInsertValues = new HashMap();

  static
  {
    defaultOrderInsertValues.put("ORDERVERSION", 33L);
    defaultOrderInsertValues.put("LASTMODIFIEDTIME", 100L);
    defaultOrderInsertValues.put("LASTUPDATETIME", 200L);
    defaultOrderInsertValues.put("ORDERFILELENGTH", 1L);
    defaultOrderInsertValues.put("STATE", 0L);
    defaultOrderInsertValues.put("RESERVATION", 0L);
    defaultOrderInsertValues.put("ORDERFILENAME", "filename");
    defaultOrderInsertValues.put("ORDERUSERNAME", "username");
    defaultOrderInsertValues.put("ORDERTYPE", "A");
    defaultOrderInsertValues.put("ORDERTYPENR", 1L);
    defaultOrderInsertValues.put("INTEGRITYSTATE", "0");
    defaultOrderInsertValues.put("ORDERISTEST", "T");
    defaultOrderInsertValues.put("FARMSPECIFIC", "farm specific");

    defaultOrderInsertValues.put("ORDERSTARTTIME", 0L);
    defaultOrderInsertValues.put("ORDERCONTEXTID", 0L);
    defaultOrderInsertValues.put("CONTEXTIDASYNCH", 0L);
    defaultOrderInsertValues.put("CURRENTCOMMANDNB", -1L);
    defaultOrderInsertValues.put("CURRENTRECNBOFCURRENTCOMMANDS", -1L);
    defaultOrderInsertValues.put("CURRENTCOMMANDPROGRESS", -1L);
    defaultOrderInsertValues.put("TOTALRECNBOFCURRENTCOMMAND", -1L);
    defaultOrderInsertValues.put("TOTALNBOFCOMMANDS", -1L);
    defaultOrderInsertValues.put("ORDERISPERIODIC", "0");
    defaultOrderInsertValues.put("ORDERISCANCELED", "0");
  }

  private HashMap defaultsTests;

  {
    defaultsTests = new HashMap()
    {{
      put("a", new HashMap());
    }};
  }

  @Test
  public void testParse() throws Exception
  {
    String inserts = "INSERT INTO PLFBSPAN_BSDRORDERS(ORDERVERSION,LastModifiedTime,LASTUPDATETIME,OrderFileLength,State,RESERVATION,OrderFileName,OrderUserName,ORDERTYPE,ORDERTYPENR,INTEGRITYSTATE,ORDERISTEST,FARMSPECIFIC)VALUES(33,?,?,?,0,0,?,?,?,?,0,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRORDERS(ORDERVERSION,LastModifiedTime,LASTUPDATETIME,OrderFileLength,State,RESERVATION,OrderFileName,OrderUserName,ORDERTYPE,ORDERTYPENR,INTEGRITYSTATE,FARMNAME,CEID)VALUES(33,?,?,?,0,0,?,?,?,?,0,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRORDERS(ORDERVERSION,LastModifiedTime,LASTUPDATETIME,OrderFileLength,State,RESERVATION,OrderFileName,OrderUserName,ORDERTYPE,ORDERTYPENR,INTEGRITYSTATE)VALUES(33,?,?,?,1,0,?,?,?,?,0)\n" +
      "INSERT INTO PLFBSPAN_BSDRGAP(ORDERUSERNAME,ORDERTYPE,CLUSTERNAME,STARTTIME,ENDTIME,LASTUPDATETIME,TYPE)VALUES(?,?,?,?,?,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRORDERIDHOLDER(DUMMY)values(2)\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME)values('audit')\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME)values('getOrder')\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME, CEID)values('getOrder',?)\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME)values('polling')\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME, CEID)values('polling',?)\n" +
      "INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME)values('bfsmAddress')\n" +
      "INSERT INTO PLFBSPAN_BSDRPERIOD (LASTEXECUTIONSTATE,LASTEXECUTIONTIME,ORDERLASTDATE,ORDERPERIOD,NUMBEROFEXECUTIONS,SCHEDULESTARTTIME,TIMESOFDAY,DAYSOFWEEK,DAYSOFMONTH,PUBLICHOLIDAYS,BANKHOLIDAYS,ADDITIONALEXECUTIONTIMES,EXECUTIONCOUNTER,IMMEDIATEEXECUTIONTIME,OrderID)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRFILES(OrderID,ADDFILETYPE,ADDFILENAME,INTEGRITYSTATE)VALUES(?,?,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRFILES(OrderID,ADDFILETYPE,ADDFILENAME,MACUSER,MACVALUE,MACDATE,MACORIGIN,MACSEQUENCEID,INTEGRITYSTATE)VALUES(?,?,?,?,?,?,?,?,?)\n" +
      "INSERT INTO PLFBSPAN_BSDRFILES(OrderID,ADDFILETYPE,ADDFILENAME,MACDATE,MACORIGIN,MACSEQUENCEID,INTEGRITYSTATE)VALUES(?,?,?,?,?,?,3)";

    String[] statements = inserts.split("\n");
    for ( String insert : statements )
    {
      spannerInsertBuilder(insert, statementDefault.getDefaultParameters());
    }
  }

  @Test
  public void shouldParseInsertStatements() throws Exception
  {
    {
      String[] parts = parserInsert("INsERT  IntO  myTable   (column1, col2, col_3, col_4) values ('a,B(())),c\\'sd',23,2019/01/02,'','',?)");
      assertEquals("myTable", parts[0]);
      assertEquals("column1, col2, col_3, col_4", parts[1]);
      assertEquals("'a,B(())),c\\'sd',23,2019/01/02,'','',?", parts[2]);
    }
    {
      Object[] result = parseValueGroup("'a,B,c\\'sd' ,23 ,2019/01/02 , '', '',?");
      assertEquals(6, result.length);
      assertEquals("a,B,c\\'sd", result[0]);
      assertEquals(Long.valueOf(23), result[1]);
      assertEquals("2019/01/02", result[2]);
      assertEquals("", result[3]);
      assertEquals("", result[4]);
      assertEquals("?", result[5]);
    }
  }

  @Test
  public void shouldBindToInsertHolders() throws RuntimeException
  {
    {
      InsertMutationHolder mutationHolder = spannerInsertBuilder("insert into a(c1,c2) values(?,?)",
        defaultsTests);
      assertEquals("C1", mutationHolder.requiredParams.get("@_1"));
      assertEquals("C2", mutationHolder.requiredParams.get("@_2"));
      mutationHolder.bind(1, "vc1");
      mutationHolder.bind(2, "vc2");
      assertTrue(mutationHolder.requiredParams.isEmpty());
      Mutation mutation = mutationHolder.build();
      assertEquals(Value.string("vc1"), mutation.asMap().get("C1"));
      assertEquals(Value.string("vc2"), mutation.asMap().get("C2"));
    }
    {
      InsertMutationHolder mutationHolder = spannerInsertBuilder("insert into a(c1,c2,c3,c4) values('asd',?,23,?)",
        defaultsTests);
      assertEquals("C2", mutationHolder.requiredParams.get("@_1"));
      assertEquals("C4", mutationHolder.requiredParams.get("@_2"));
      mutationHolder.bind(1, "vc2v");
      mutationHolder.bind(2, "vc4v");
      assertTrue(mutationHolder.requiredParams.isEmpty());
      Mutation mutation = mutationHolder.build();
      assertEquals("a", mutation.getTable());
      assertEquals(Value.string("asd"), mutation.asMap().get("C1"));
      assertEquals(Value.string("vc2v"), mutation.asMap().get("C2"));
      assertEquals(Value.int64(23), mutation.asMap().get("C3"));
      assertEquals(Value.string("vc4v"), mutation.asMap().get("C4"));
    }
  }

  @Test
  public void shouldinsertOrder() throws Exception
  {
    getClient().readWriteTransaction().run(transaction ->
    {
      insertOrder(transaction);
      return null;
    });

    getClient().readWriteTransaction().run(transaction ->
    {
      ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRORDERS").execute(transaction);
      assertTrue(rs.next());

      assertFieldsMatch(rs, defaultOrderInsertValues);

      transaction.buffer(
        Mutation.delete("PLFBSPAN_BSDRORDERS", Key.of(rs.getLong("ORDERID"))));
      return null;
    });
  }

  @Test
  public void insertGap() throws Exception
  {
    String username = "username";
    String ordertype = "ordertype";
    String clustername = "clustername";
    String starttime = "starttime";
    String endtime = "endtime";
    long lastUpdateTime = 1l;
    long type = 2l;
    InsertMutationHolder statement = (InsertMutationHolder) spannerInsertBuilder("INSERT INTO PLFBSPAN_BSDRGAP(ORDERUSERNAME,ORDERTYPE,CLUSTERNAME,STARTTIME,ENDTIME,LASTUPDATETIME,TYPE)VALUES(?,?,?,?,?,?,?)",
      statementDefault.getDefaultParameters())
      .bind(1, username)
      .bind(2, ordertype)
      .bind(3, clustername)
      .bind(4, starttime)
      .bind(5, endtime)
      .bind(6, lastUpdateTime)
      .bind(7, type);
    Mutation mutation = statement.build();
    long generatedId = mutation.asMap().get("SEQ").getInt64();
    statement.execute(getClient());

    ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRGAP where SEQ=?").bind(1, generatedId).execute(getClient());
    assertTrue(rs.next());
    assertEquals(username, rs.getString("ORDERUSERNAME"));
    assertEquals(ordertype, rs.getString("ORDERTYPE"));
    assertEquals(clustername, rs.getString("CLUSTERNAME"));
    assertEquals(starttime, rs.getString("STARTTIME"));
    assertEquals(endtime, rs.getString("ENDTIME"));
    assertEquals(lastUpdateTime, rs.getLong("LASTUPDATETIME"));
    assertEquals(type, rs.getLong("TYPE"));
  }

  @Test
  public void config() throws Exception
  {
    long ceid = 1l;
    InsertMutationHolder statement = (InsertMutationHolder) spannerInsertBuilder("INSERT INTO PLFBSPAN_BSDRCONFIG(LOCKNAME, CEID)values('polling',?)",
      statementDefault.getDefaultParameters())
      .bind(1, ceid);

    Mutation mutation = statement.build();
    long generatedId = mutation.asMap().get("SEQ").getInt64();
    statement.execute(getClient());

    ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRCONFIG where SEQ=?").bind(1, generatedId).execute(getClient());
    assertTrue(rs.next());
    assertEquals("polling", rs.getString("LOCKNAME"));
    assertEquals(ceid, rs.getLong("CEID"));
  }

  @Test
  public void insertPeriod() throws Exception
  {
    long orderId = getClient().readWriteTransaction().run(this::insertOrder);

    long lastExecutionState = 1L;
    long lastExecutionTime = 2L;
    long orderLastDate = 3L;
    long orderPeriod = 4L;
    long numberOfExecutions = 4L;
    long scheduleStartTime = 4L;
    long executioncounter = 4L;
    long immediateexecutiontime = 4L;

    String timesofday = "TIMESOFDAY";
    String daysofweek = "DAYSOFWEEK";
    String daysofmonth = "DAYSOFMONTH";
    String publicholidays = "1";
    String bankholidays = "1";
    String additionalexecutiontimes = "ADDITIONALEXECUTIONTIMES";
    InsertMutationHolder statement = (InsertMutationHolder) spannerInsertBuilder("INSERT INTO PLFBSPAN_BSDRPERIOD (LASTEXECUTIONSTATE,LASTEXECUTIONTIME,ORDERLASTDATE,ORDERPERIOD,NUMBEROFEXECUTIONS,SCHEDULESTARTTIME," +
        "TIMESOFDAY,DAYSOFWEEK,DAYSOFMONTH,PUBLICHOLIDAYS,BANKHOLIDAYS,ADDITIONALEXECUTIONTIMES,EXECUTIONCOUNTER,IMMEDIATEEXECUTIONTIME,OrderID)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
      statementDefault.getDefaultParameters())
      .bind(1, lastExecutionState)
      .bind(2, lastExecutionTime)
      .bind(3, orderLastDate)
      .bind(4, orderPeriod)
      .bind(5, numberOfExecutions)
      .bind(6, scheduleStartTime)
      .bind(7, timesofday)
      .bind(8, daysofweek)
      .bind(9, daysofmonth)
      .bind(10, publicholidays)
      .bind(11, bankholidays)
      .bind(12, additionalexecutiontimes)
      .bind(13, executioncounter)
      .bind(14, immediateexecutiontime)
      .bind(15, orderId)
      ;

    Mutation mutation = statement.build();
    long generatedId = mutation.asMap().get("SEQ").getInt64();
    statement.execute(getClient());

    ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRPERIOD where SEQ=?").bind(1, generatedId).execute(getClient());
    assertTrue(rs.next());
    assertEquals(lastExecutionState, rs.getLong("LASTEXECUTIONSTATE"));
    assertEquals(lastExecutionTime, rs.getLong("LASTEXECUTIONTIME"));
    assertEquals(orderLastDate, rs.getLong("ORDERLASTDATE"));
    assertEquals(orderPeriod, rs.getLong("ORDERPERIOD"));
    assertEquals(numberOfExecutions, rs.getLong("NUMBEROFEXECUTIONS"));
    assertEquals(scheduleStartTime, rs.getLong("SCHEDULESTARTTIME"));
    assertEquals(timesofday, rs.getString("TIMESOFDAY"));
    assertEquals(daysofweek, rs.getString("DAYSOFWEEK"));
    assertEquals(daysofmonth, rs.getString("DAYSOFMONTH"));
    assertEquals(publicholidays, rs.getString("PUBLICHOLIDAYS"));
    assertEquals(bankholidays, rs.getString("BANKHOLIDAYS"));
    assertEquals(additionalexecutiontimes, rs.getString("ADDITIONALEXECUTIONTIMES"));
    assertEquals(executioncounter, rs.getLong("EXECUTIONCOUNTER"));
    assertEquals(immediateexecutiontime, rs.getLong("IMMEDIATEEXECUTIONTIME"));
    assertEquals(orderId, rs.getLong("ORDERID"));
  }

  @Test
  public void insertFile() throws Exception
  {
    long orderId = getClient().readWriteTransaction().run(this::insertOrder);
    long addfiletype = 1L;
    String addfilename = "ADDFILENAME";
    String integritystate = "I";
    InsertMutationHolder statement = (InsertMutationHolder) spannerInsertBuilder("INSERT INTO PLFBSPAN_BSDRFILES(OrderID,ADDFILETYPE,ADDFILENAME,INTEGRITYSTATE)VALUES(?,?,?,?)",
      statementDefault.getDefaultParameters())
      .bind(1, orderId)
      .bind(2, addfiletype)
      .bind(3, addfilename)
      .bind(4, integritystate)
      ;

    Mutation mutation = statement.build();
    long generatedId = mutation.asMap().get("SEQ").getInt64();
    statement.execute(getClient());

    ResultSet rs = spannerQueryBuilder("select * from PLFBSPAN_BSDRFILES where SEQ=?").bind(1, generatedId).execute(getClient());
    assertTrue(rs.next());
    assertEquals(orderId, rs.getLong("ORDERID"));
    assertEquals(addfiletype, rs.getLong("ADDFILETYPE"));
    assertEquals(addfilename, rs.getString("ADDFILENAME"));
    assertEquals(integritystate, rs.getString("INTEGRITYSTATE"));
  }

}*/
