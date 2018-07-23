

package com.excellenceengineeringsolutions;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import oracle.jdbc.OracleDriver;

public class ReadATFRTFromOracle
{
  public static final String CN = "InsertExample";
  public static final String CNP = CN + ".";


  @Test
  public void readTest() throws Exception
  {
    DriverManager.registerDriver(new OracleDriver());
    try(Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@//10.231.196.185:1533/CUSTDB","adv","2Ab}7C"))
    {
      PreparedStatement preparedStatement = null;

      String ratePlanId = "1-200_005_0044";
      String[] tables = getTables(connection,"ATFRT%");

      List<String> tablesWithBoth = new ArrayList<>();
      List<String> tablesWithCUSTID = new ArrayList<>();
      List<String> tablesWithParentId = new ArrayList<>();


      long count = 0;
      for(String table : tables) {
        System.out.println("table: " + table);
        boolean hasCustomerId = hasColumn(connection, table, "CUSTID");
        boolean hasParentId = hasColumn(connection, table, "ParentId");
        if (hasCustomerId && hasParentId){
          preparedStatement =
                  connection.prepareStatement(String.format("select count(*) from %s " +
                  "where CUSTID=? or ParentId = ?",table));
          preparedStatement.setString(1, ratePlanId);
          preparedStatement.setString(2, ratePlanId);
          tablesWithBoth.add(table);
        }else if (hasCustomerId){
          preparedStatement =
                  connection.prepareStatement(String.format("select count(*) from %s " +
                          "where CUSTID=?",table));
          preparedStatement.setString(1, ratePlanId);
          tablesWithCUSTID.add(table);
        }else if (hasParentId){
          preparedStatement =
                  connection.prepareStatement((String.format("select count(*) from %s " +
                          "where ParentId = ?",table)));
          preparedStatement.setString(1, ratePlanId);
          tablesWithParentId.add(table);
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        long found = resultSet.getLong(1);
        count+=found;
        System.out.println("count: " + found);
      }
      System.out.println("Tables with key: " + count);
      System.out.println("tablesWithBoth " + tablesWithBoth);
      System.out.println("tablesWithCUSTID " + tablesWithCUSTID);
      System.out.println("tablesWithParentId " + tablesWithParentId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private String[] getTables(Connection connection, String tablePrefix) throws SQLException {
    List<String> result = new ArrayList();
    PreparedStatement prepareStatement = connection.prepareStatement("select table_name from user_tables where upper(table_name) like ?");
           prepareStatement.setString(1, tablePrefix.toUpperCase());
    ResultSet resultSet = prepareStatement.executeQuery();
    while(resultSet.next()){
     result.add(resultSet.getString(1));
    }
    return result.toArray(new String[result.size()]);
  }

  public boolean hasColumn(Connection connection, String tableName, String columnName) throws SQLException{
    PreparedStatement prepareStatement = connection.prepareStatement("select * from user_tab_columns where upper(table_name) = ? and upper(column_name)=?");
    prepareStatement.setString(1, tableName.toUpperCase());
    prepareStatement.setString(2, columnName.toUpperCase());
    ResultSet resultSet = prepareStatement.executeQuery();
    return resultSet.next();
  }
}
