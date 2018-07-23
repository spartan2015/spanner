

package com.excellenceengineeringsolutions.spannerjdbc;

import com.excellenceengineeringsolutions.AppException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.DYNAMIC;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.TABLE_NAME_REG_EXP;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.getKeys;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.join;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.parseValue;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.setToBuilderFromResultSet;
import static com.excellenceengineeringsolutions.spannerjdbc.StatementSelectHandler.spannerQueryBuilder;

/**
 * Handling sql update statement through Spanner Client
 */
public class StatementUpdateHandler
{

  private static Pattern UPDATE_REGEXP = Pattern.compile("(?i)update[ \n]+(" + TABLE_NAME_REG_EXP + ")" + "[ \n]+SET[ \n]+" + "(.+)" + "[ \n]+(?:WHERE[ \n]+" + "(.*))");

  // force non-instantiability through the `private` constructor
  private StatementUpdateHandler()
  {
    throw new AssertionError("This class cannot be instantiated");
    // or, throw `UnsupportedOperationException`
  }

  public static UpdateMutationHolder spannerUpdateBuilder(
                                                          String updateQuery) throws AppException
  {
    return spannerUpdateBuilder(null,updateQuery);
  }

  public static UpdateMutationHolder spannerUpdateBuilder(List<String> selectKeys,
                                                          String updateQuery) throws AppException
  {
    String[] updateParts = parserUpdate(updateQuery);
    AtomicInteger paramIndex = new AtomicInteger(1);
    UpdateParams updateSetMapHolder = getUpdateSetMap(updateParts[1], paramIndex);
    int fromParamIndex = paramIndex.get();
    StatementSelectHandler.SelectStatementHolder selectStatementHolder = spannerQueryBuilder(
      "select " + join(selectKeys) + " from " + updateParts[0] + " where " + updateParts[2], paramIndex);
    Map<String, Object> requiredSelectParams = new HashMap();
    for ( int i = fromParamIndex; i < paramIndex.get(); i++ )
    {
      requiredSelectParams.put("_" + i, null);
    }

    return new UpdateMutationHolder(updateQuery,
      updateParts[0], selectKeys, selectStatementHolder,
      requiredSelectParams, updateSetMapHolder.givenParams, updateSetMapHolder.indexToColumn);
  }

  static String[] parserUpdate(String updateQuery) throws AppException
  {
    Matcher matcher = UPDATE_REGEXP.matcher(updateQuery);
    if ( matcher.find() )
    {
      return new String[]{matcher.group(1), matcher.group(2), matcher.group(3)};
    } else
    {
      throw new AppException(String.format("Could not extract table name from query [%s]", updateQuery));
    }
  }

  private static UpdateParams getUpdateSetMap(String updatePart, AtomicInteger paramIndex)
  {
    Map<String, Object> givenParams = new HashMap<>();
    Map<String, String> indexToColumn = new HashMap<>();
    for ( String fieldSet : updatePart.split(",") )
    {
      String[] fieldSetParts = fieldSet.split("=");
      Object value = parseValue(fieldSetParts[1]);
      if ( !"?".equals(value) )
      {
        givenParams.put(fieldSetParts[0].trim().toUpperCase(), value);
      } else
      {
        indexToColumn.put("_" + paramIndex.getAndIncrement(), fieldSetParts[0].trim().toUpperCase());
      }
    }
    return new UpdateParams(givenParams, indexToColumn);
  }

  public static class UpdateMutationHolder implements SpannerMutationStatement
  {
    private String query;
    private String tableName;
    private List<String> keys;
    private StatementSelectHandler.SelectStatementHolder selectStatementHolder;
    private Map<String, Object> requiredSelectParams;
    private Map<String, Object> givenUpdateParams;
    private Map<String, Object> bindedUpdateParams = new HashMap<>();
    private Map<String, String> indexToColumn;
    private int changeCount = 0;

    public UpdateMutationHolder(String query,
                                String tableName,
                                List<String> keys,
                                StatementSelectHandler.SelectStatementHolder selectStatementHolder,
                                Map<String, Object> requiredSelectParams,
                                Map<String, Object> givenUpdateParams,
                                Map<String, String> indexToColumn)
    {
      this.tableName = tableName;
      this.keys = keys;
      this.query = query;
      this.selectStatementHolder = selectStatementHolder;
      this.givenUpdateParams = givenUpdateParams;
      this.requiredSelectParams = requiredSelectParams;
      this.bindedUpdateParams = bindedUpdateParams;
      this.indexToColumn = indexToColumn;
    }

    /**
     * bind positional value to executed statement
     *
     * @param paramNo    starting with 1
     * @param paramValue
     * @return
     */
    public SpannerMutationStatement bind(int paramNo, Object paramValue)
    {
      Preconditions.checkArgument(paramNo > 0, "Indexes start from 1");
      String key = "_" + paramNo;
      bind(key, paramValue);
      return this;
    }

    public SpannerMutationStatement bind(String key, Object paramValue)
    {
      Map<String, Object> params = requiredSelectParams.containsKey(key) ? requiredSelectParams : bindedUpdateParams;
      params.put(key, paramValue);
      return this;
    }

    @Override
    public void execute(DatabaseClient client) throws AppException
    {
      executeUpdate(client);
    }

    @Override
    public void execute(TransactionContext transaction) throws AppException
    {
      executeUpdate(transaction);
    }

    /**
     * return the number of updated rows
     *
     * @param client
     * @return
     */
    public int executeUpdate(DatabaseClient client)
    {
      int index = -1;
      if ((keys==null || keys.isEmpty()) && (index = selectStatementHolder.query.indexOf(DYNAMIC))!=-1){
        keys = getKeys(client, tableName);
        if (keys==null || keys.isEmpty()){
          throw new RuntimeException("No keys found for table: " + tableName);
        }
        selectStatementHolder.query.replace(index, index+DYNAMIC.length(),join(keys));
      }
      return client.readWriteTransaction().run(transaction ->
      {
        return executeUpdate(transaction);
      });
    }

    public Integer executeUpdate(TransactionContext transaction)
    {
      int index = -1;
      if ((index = selectStatementHolder.query.indexOf(DYNAMIC))!=-1){
        throw new RuntimeException("Cannot dynamically fetch keys for query in a read write transaction. You must specify the keys!");
      }
      changeCount = 0;
      for ( String param : requiredSelectParams.keySet() )
      {
        selectStatementHolder.bind(param, requiredSelectParams.get(param));
      }
      try ( ResultSet rs = selectStatementHolder.execute(transaction) )
      {
        while ( rs.next() )
        {
          Mutation.WriteBuilder builder = Mutation.newUpdateBuilder(tableName);
          for ( String key : keys )
          {
            rs.getCurrentRowAsStruct().getColumnType(key);
            setToBuilderFromResultSet(builder, rs, key);
          }
          for ( String column : givenUpdateParams.keySet() )
          {
            Object paramValue = givenUpdateParams.get(column);
            StatementHandlerCommon.bindParam(builder, column, paramValue);
          }
          for ( String paramIndex : bindedUpdateParams.keySet() )
          {
            Object paramValue = bindedUpdateParams.get(paramIndex);
            StatementHandlerCommon.bindParam(builder, indexToColumn.get(paramIndex), paramValue);
          }
          transaction.buffer(builder.build());
          changeCount++;
        }
      }
      return changeCount;

    }

    public int getChangeCount()
    {
      return changeCount;
    }

    @Override
    public String toString()
    {
      return "UpdateMutationHolder{" +
        "query='" + query + '\'' +
        ", tableName='" + tableName + '\'' +
        ", keys=" + keys +
        ", selectStatementHolder=" + selectStatementHolder +
        ", requiredSelectParams=" + requiredSelectParams +
        ", givenUpdateParams=" + givenUpdateParams +
        ", bindedUpdateParams=" + bindedUpdateParams +
        ", indexToColumn=" + indexToColumn +
        '}';
    }
  }

  private static class UpdateParams
  {
    Map<String, Object> givenParams;
    Map<String, String> indexToColumn;

    public UpdateParams(Map<String, Object> givenParams, Map<String, String> indexToColumn)
    {
      this.givenParams = givenParams;
      this.indexToColumn = indexToColumn;
    }
  }
}
