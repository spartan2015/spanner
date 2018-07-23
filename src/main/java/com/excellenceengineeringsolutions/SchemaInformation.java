
package com.excellenceengineeringsolutions;

import com.google.cloud.RetryOption;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;
import org.threeten.bp.Duration;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.*;

public class SchemaInformation
{
@Test
public void t1(){
  parseType("ARRAY<STRING(20)>");
}
  private static void parseType(String actualType)
  {
    final int notFoundIndex = -1;
    final int openParanthesisIndex = actualType.indexOf('(');
    final boolean actualHasTypeSize = openParanthesisIndex != notFoundIndex;
    final int closingParanthesisIndex = actualHasTypeSize ? actualType.indexOf(')') : notFoundIndex;
    if ( actualHasTypeSize &&
      (closingParanthesisIndex == notFoundIndex
        || closingParanthesisIndex != actualType.lastIndexOf(')')
        || !actualType.substring(closingParanthesisIndex + 1).trim().isEmpty()) )
    {
      throw new IllegalArgumentException(String.format("Wrong format of database column type. " + "Expected format is 'COLUMNTYPE(SIZE)' but was '%s'", actualType));
    }
  }


/*
    public void setDbComment(ITransactionManager var1, String var2, String var3, int var4) {
        this.setDbComment(var1, var2, var3, var4, (String)null);
    }

    public void setDbComment(ITransactionManager var1, String var2, String var3, int var4, String var5) {
        PreparedStatement var6 = null;
        Connection var7 = ((ITransactionManagerImpl)var1).getConnection();

        try {
            if (var5 == null) {
                var5 = "???";
            }

            if (var2 != null) {
                String var8 = null;
                if (!var5.equals("Counter") && !var5.equals("CounterArray") && !var5.equals("CounterArray1") && !var5.equals("CounterArray2")) {
                    if (var5.equals("Account") || var5.equals("OnCounter") || var5.equals("StCounter") || var5.equals("C0Array") || var5.equals("C1Array") || var5.equals("C2Array")) {
                        var8 = "COMMENT ON COLUMN " + var3 + "." + var2 + " IS 'CV=2;DMV=" + var4 + ";T=" + var5.substring(0, 3) + "'";
                    }
                } else {
                    var8 = "COMMENT ON COLUMN " + var3 + "." + var2 + " IS 'CV=1;DMV=" + var4 + "'";
                }

                if (var8 != null) {
                    var6 = var7.prepareStatement(var8);
                    IFrwTracer var9 = this.mSdfHome.getContainer().tracer();
                    if (var9.isTraceOn(108930815987056644L, 4)) {
                        String var10 = "SDF:DB:ServiceManagementSupport.setDbComment(): " + var8;
                        var9.trace(this.mSdfHome.getComponentHandle(), 108930815987056644L, 4, var10);
                    }

                    var6.execute();
                    var6.close();
                    var6 = null;
                    if (var5.equals("C0Array")) {
                        this.setDbComment(var1, var2.substring(0, 1) + "1" + var2.substring(2, var2.length()), var3, var4, "C1Array");
                        this.setDbComment(var1, var2.substring(0, 1) + "2" + var2.substring(2, var2.length()), var3, var4, "C2Array");
                    }
                }
            }
        } catch (SQLException var21) {
            this.mSdfHome.notifyError("ServiceManagementSupport.setDbComment(): ignored SQLException: " + var21.getMessage());
        } catch (Exception var22) {
            this.mSdfHome.notifyError("ServiceManagementSupport.setDbComment(): ignored Exception: " + var22.getMessage());
        } finally {
            try {
                if (var6 != null) {
                    var6.close();
                }
            } catch (SQLException var20) {
                this.mSdfHome.notifyError("ServiceManagementSupport.setDbComment(): ignored SQLException in finally block: " + var20.getMessage());
            }

        }

    }

    public int getVersionFromDbComment(ITransactionManager var1, String var2, String var3) {
        IFrwTracer var4 = this.mSdfHome.getContainer().tracer();
        PreparedStatement var5 = null;
        int var6 = 0;

        try {
            Connection var7 = ((ITransactionManagerImpl)var1).getConnection();
            String var8;
            if (var2 != null) {
                var5 = var7.prepareStatement("select comments from user_col_comments where table_name = ? and column_name = ?");
                var5.setString(1, var3.toUpperCase());
                var5.setString(2, var2.toUpperCase());
                if (var4.isTraceOn(108930815987056644L, 4)) {
                    var8 = "SDF:DB:ServiceManagementSupport.getVersionFromDbComment():  param[0]=" + var3.toUpperCase() + " param[1]=" + var2.toUpperCase();
                    var4.trace(this.mSdfHome.getComponentHandle(), 108930815987056644L, 4, var8);
                }
            } else {
                var5 = var7.prepareStatement("select comments from user_tab_comments where table_name = ?");
                var5.setString(1, var3.toUpperCase());
                if (var4.isTraceOn(108930815987056644L, 4)) {
                    var8 = "SDF:DB:ServiceManagementSupport.getVersionFromDbComment():  param[0]=" + var3.toUpperCase();
                    var4.trace(this.mSdfHome.getComponentHandle(), 108930815987056644L, 4, var8);
                }
            }

            java.sql.ResultSet var26 = var5.executeQuery();
            if (var26.next()) {
                String var9 = var26.getString("comments");
                if (var9 != null) {
                    char[] var10 = var9.toCharArray();
                    if (var10[0] == 'C' && var10[1] == 'V' && var10[2] == '=') {
                        if (var10[3] == '1') {
                            var6 = Integer.parseInt(var9.substring(9));
                        }

                        if (var10[3] == '2') {
                            int var11 = 9;

                            for(int var12 = var11; var12 < var9.length() && '0' <= var10[var12] && var10[var12] <= '9'; ++var12) {
                                ++var11;
                            }

                            var6 = Integer.parseInt(var9.substring(9, var11));
                        }
                    }
                }
            }
        } catch (SQLException var23) {
            this.mSdfHome.notifyError("ServiceManagementSupport.getVersionFromDbComment(): ignored SQLException: " + var23.getMessage());
        } catch (Exception var24) {
            this.mSdfHome.notifyError("ServiceManagementSupport.getVersionFromDbComment(): ignored Exception: " + var24.getMessage());
        } finally {
            try {
                if (var5 != null) {
                    var5.close();
                }
            } catch (SQLException var22) {
                this.mSdfHome.notifyError("ServiceManagementSupport.getVersionFromDbComment(): ignored SQLException in finally block: " + var22.getMessage());
            }

        }

        return var6;
    }

    */

  public static String checkIfIndexExist(/*SdfHome mSdfHome,*/ DatabaseClient client, String
    tableName, String[] indexColumns)
  {
    Statement var4 = null;
    int currentPosition = 0;
    boolean found = false;
    String currentIndexName = null;

    ResultSet resultSet = null;
    try
    {
      String sqlQuery = null;

      sqlQuery = "SELECT index_name,column_name,ordinal_position FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
        "where index_name!='PRIMARY_KEY' and table_name = @table_name order by index_name,ordinal_position";


      resultSet = client.singleUse().executeQuery(Statement.newBuilder(sqlQuery)
        .bind("table_name").to(tableName.toUpperCase())
        .build());

      while ( resultSet.next() )
      {
        String indexName = resultSet.getString(0);
        String columnName = resultSet.getString(1);
        int position = (int) resultSet.getLong(2);

        while ( true )
        {
          found = false;
          if ( currentPosition == 0 )
          {
            currentIndexName = indexName;
          }

          if ( currentIndexName.equalsIgnoreCase(indexName) )
          {
            if ( currentPosition < indexColumns.length )
            {
              if ( columnName.equalsIgnoreCase(indexColumns[currentPosition]) && position == currentPosition + 1 )
              {
                ++currentPosition;
              } else
              {
                currentPosition = 0;
              }
            } else
            {
              currentPosition = 0;
            }
          } else
          {
            if ( currentPosition == indexColumns.length )
            {
              return currentIndexName;
            }
            currentPosition = 0;
            found = true;
          }

          if ( !found )
          {
            break;
          }
        }
      }

      resultSet.close();
    }
    catch ( SpannerException ex )
    {
//            mSdfHome.notifyError("ServiceManagementSupport.checkIfIndexExist(): ignored SQLException: " + ex.getMessage());
    }
    catch ( Exception ex )
    {
//            mSdfHome.notifyError("ServiceManagementSupport.checkIfIndexExist(): ignored Exception: " + ex.getMessage());
    }
    finally
    {
      if ( resultSet != null )
      {
        resultSet.close();
      }
    }
    if ( currentPosition == indexColumns.length )
    {
      return currentIndexName;
    } else
    {
      return null;
    }
  }

  public static boolean checkIfIndexExist(/*SdfHome mSdfHome,*/ DatabaseClient client, String indexName, String tableName, String columnName, int columnPosition)
  {
    Statement.Builder statementBuilder = null;
    boolean found = false;

    try
    {
      StringBuilder sql = null;

      sql = new StringBuilder("SELECT count(*) FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
        "where index_name!='PRIMARY_KEY' " +
        "and table_name = @table_name " +
        "and index_name = @index_name ");

      if ( columnName != null )
      {
        sql.append(" and column_name=@column_name ");
      }
      if ( columnPosition > 0 )
      {
        sql.append(" and ordinal_position=@ordinal_position");
      }
      statementBuilder = Statement.newBuilder(sql.toString());
      statementBuilder.bind("table_name").to(tableName.toUpperCase());
      statementBuilder.bind("index_name").to(indexName.toUpperCase());
      if ( columnName != null )
      {
        statementBuilder.bind("column_name").to(columnName.toUpperCase());
      }

      if ( columnPosition > 0 )
      {
        statementBuilder.bind("ordinal_position").to(columnPosition);
      }

            /*IFrwTracer tracer = mSdfHome.getContainer().tracer();
            if (tracer.isTraceOn(108930815987056644L, 4)) {
                String var10 = "SDF:DB:ServiceManagementSupport.checkIfIndexExist(): " + sql + " param[0]=" + indexName.toUpperCase() + " param[1]=" + tableName.toUpperCase() + (columnName != null ? " param[2]=" + columnName.toUpperCase() : "") + (columnPosition > 0 ? " param[3]=" + columnPosition : "");
                tracer.trace(mSdfHome.getComponentHandle(), 108930815987056644L, 4, var10);
            }*/

      ResultSet resultSet = client.singleUse().executeQuery(statementBuilder.build());
      if ( resultSet.next() && resultSet.getLong(0) > 0 )
      {
        found = true;
      }

    }
    catch ( SpannerException ex )
    {
//            mSdfHome.notifyError("ServiceManagementSupport.checkIfIndexExist(): ignored SQLException: " + ex.toString());
    }
    catch ( Exception ex )
    {
//            mSdfHome.notifyError("ServiceManagementSupport.checkIfIndexExist(): ignored Exception: " + ex.getMessage());
    }

    return found;
  }

  public static String getNextFreeIndexName(/*SdfHome mSdfHome,*/ DatabaseClient client, String indexName, String tableName, String uniqueness)
  {
    int start = 0;
    boolean isUnique = "UNIQUE".equalsIgnoreCase(uniqueness);
    if ( uniqueness != null && uniqueness.equalsIgnoreCase("NONUNIQUE") )
    {
      start = 50;
    }

    if ( indexName != null && tableName != null && indexName.toUpperCase().startsWith("AU") && indexName.toUpperCase().endsWith(tableName.toUpperCase().substring(3)) )
    {
      String newIndexName = null;

      try
      {
        String sql = null;

        sql = "select index_name from information_schema.indexes" +
          " where index_type='INDEX'" +
          " and table_name=@tableName" +
          " and IS_UNIQUE = @isUnique" +
          " and index_name LIKE 'AU%'" +
          " order by index_name desc";
        Statement statement = Statement.newBuilder(sql)
          .bind("tableName").to(tableName)
          .bind("isUnique").to(isUnique)
          .build();

                /*IFrwTracer tracer = mSdfHome.getContainer().tracer();
                if (tracer.isTraceOn(108930815987056644L, 4)) {
                    tracer.trace(mSdfHome.getComponentHandle(), 108930815987056644L, 4, "SDF:DB:ServiceManagementSupport.getNextFreeIndexName(): " + sql);
                }*/

        String currentIndexName = null;
        ResultSet resultSet = client.singleUse().executeQuery(statement);
        if ( resultSet.next() )
        {
          currentIndexName = resultSet.getString(0);
          start = Integer.parseInt(currentIndexName.substring(2, 4));
          ++start;
        } else
        {
          currentIndexName = indexName;
        }

        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(2);
        newIndexName = currentIndexName.substring(0, 2) + numberFormat.format((long) start) + currentIndexName.substring(4);
      }
      catch ( SpannerException ex )
      {
//                mSdfHome.notifyError("ServiceManagementSupport.getNextFreeIndexName(): ignored SpannerException: " + ex.toString());
      }
      catch ( Exception ex )
      {
//                mSdfHome.notifyError("ServiceManagementSupport.getNextFreeIndexName(): ignored Exception: " + ex.getMessage());
      }

      return newIndexName;
    } else
    {
      return indexName;
    }
  }

    /*public static boolean checkIfTableExist(IFrwContainer container, IFrwComponentHandle mComponentHandle,IFrwTracer tracer, DatabaseClient client, String tableName) throws RuntimeException {
        boolean exists;
        try {
            if (tracer.isTraceOn(108930815987056644L, 4)) {
                tracer.trace(mComponentHandle, 108930815987056644L, 4, "SDF:DB:checkIfTableExist(): " + tableName.toUpperCase());
            }

            ResultSet resultSet = client
                    .singleUse()
                    .executeQuery(Statement
                    .newBuilder("SELECT 1 FROM information_schema.tables AS t WHERE t.table_name = @tableName")
                    .bind("tableName").to(tableName)
                    .build());
            exists = resultSet.next();
        } catch (SpannerException ex) {
            container.supervision().notifyError(mComponentHandle, 1, 1, 1L,"checkIfTableExist(): ignored SpannerException: " + ex.toString());
            throw new RuntimeException(container,mComponentHandle,"checkIfTableExist(): ignored SpannerException: " + ex.toString());
        }
        return exists;
    }*/

  @Test
  public void testSchema() throws Exception
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
    Spanner spanner = null;
    try
    {
      spanner = options.getService();
      String instanceId = "eu-instance";
      String databaseId = "vasile";
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      String index = checkIfIndexExist(dbClient, "SPAN_ACC_TEST", new String[]{"ENTITY_PK_ATTRS", "ENTITY_DEF_ID"});
      System.out.println(index);
      assertNotNull(index);

      index = checkIfIndexExist(dbClient, "SPAN_ACC_TEST", new String[]{"ENTITY_PK_ATTRS", "ENTITY_DEF_ID1"});
      System.out.println(index);
      assertNull(index);

      index = checkIfIndexExist(dbClient, "SPAN_ACC_TEST", new String[]{"ENTITY_PK_ATTRS"});
      System.out.println(index);
      assertNull(index);

      index = checkIfIndexExist(dbClient, "SPAN_ACC_TEST", new String[]{"ENTITY_DEF_ID", "ENTITY_PK_ATTRS"});
      assertNull(index);

      System.out.println(index);

      assertTrue(
        checkIfIndexExist(dbClient, "IDX_SPAN_ENT_DEF_ID_TEST", "SPAN_ENT_TEST", "ENTITY_DEF_ID", 1)
      );

      assertTrue(
        checkIfIndexExist(dbClient, "IDX_SPAN_ENT_DEF_ID_TEST", "SPAN_ENT_TEST", "ENTITY_DEF_ID", 0)
      );

      assertFalse(
        checkIfIndexExist(dbClient, "IDX_SPAN_ENT_DEF_ID_TEST", "SPAN_ENT_TEST", "ENTITY_DEF_ID", 2)
      );

      assertFalse(
        checkIfIndexExist(dbClient, "IDX_SPAN_ENT_DEF_ID_TEST", "SPAN_ENT_TEST", "CONTAINER_ID", 0)
      );


      spanner.getDatabaseAdminClient().updateDatabaseDdl(instanceId, databaseId,
        Arrays.asList("CREATE INDEX AU05SPAN_ENT_TEST on SPAN_ENT_TEST(CATEGORY)"),
        null).waitFor(RetryOption.totalTimeout(Duration.ofMinutes(1)));


      index = getNextFreeIndexName(dbClient, "AU05SPAN_ENT_TEST", "SPAN_ENT_TEST", "NONUNIQUE");
      assertEquals("AU06SPAN_ENT_TEST", index);
    }
    finally
    {
      if ( spanner != null )
      {
        spanner.close();
      }
    }
  }

}
