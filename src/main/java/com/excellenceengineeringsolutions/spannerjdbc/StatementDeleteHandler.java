

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.excellenceengineeringsolutions.spanner.StatementSelectHandler;
import com.excellenceengineeringsolutions.db.intf.SdfException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.DYNAMIC;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.TABLE_NAME_REG_EXP;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.getKeys;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.join;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.setToBuilderFromResultSet;
import static com.excellenceengineeringsolutions.spanner.StatementSelectHandler.spannerQueryBuilder;


/**
 * Handling delete sql statements through Spanner Client
 */
public class StatementDeleteHandler
{

  private static final Pattern DELETE_REGEXP = Pattern.compile("(?i)DELETE[ \n]+FROM[ \n]+(" + TABLE_NAME_REG_EXP + ")" + "([ \n]+WHERE[ \n]+" + "(.*))?");

  // force non-instantiability through the `private` constructor
  private StatementDeleteHandler()
  {
    throw new AssertionError("This class cannot be instantiated");
    // or, throw `UnsupportedOperationException`
  }

  public static DeleteMutationHolder spannerDeleteBuilder(
                                                          String deleteQuery) throws SdfException
  {
    return spannerDeleteBuilder(null, deleteQuery);
  }

  public static DeleteMutationHolder spannerDeleteBuilder(List<String> selectKeys,
                                                          String deleteQuery) throws SdfException
  {
    String[] queryParts = parserDelete(deleteQuery);
    AtomicInteger paramIndex = new AtomicInteger(1);
    com.excellenceengineeringsolutions.spanner.StatementSelectHandler.SelectStatementHolder selectStatementHolder = spannerQueryBuilder(
      "select " + join(selectKeys) + " from " + queryParts[0] +
        (queryParts.length == 2 ? " where " + queryParts[1] : ""), paramIndex);
    return new DeleteMutationHolder(deleteQuery,
      queryParts[0], selectKeys, selectStatementHolder);
  }



  static String[] parserDelete(String updateQuery) throws SdfException
  {
    Matcher matcher = DELETE_REGEXP.matcher(updateQuery);
    if ( matcher.find() )
    {
      if ( matcher.group(3) == null )
      {
        return new String[]{matcher.group(1)};
      } else
      {
        return new String[]{matcher.group(1), matcher.group(3)};
      }
    } else
    {
      throw new SdfException(String.format("Could not extract table name from query [%s]", updateQuery));
    }
  }

  public static class DeleteMutationHolder
  {
    private String query;
    private String tableName;
    private List<String> keys;
    private com.excellenceengineeringsolutions.spanner.StatementSelectHandler.SelectStatementHolder selectStatementHolder;

    public DeleteMutationHolder(String query,
                                String tableName,
                                List<String> keys,
                                StatementSelectHandler.SelectStatementHolder selectStatementHolder
    )
    {
      this.tableName = tableName;
      this.keys = keys;
      this.query = query;
      this.selectStatementHolder = selectStatementHolder;
    }

    public DeleteMutationHolder bind(int paramNo, Object paramValue)
    {
      selectStatementHolder.bind(paramNo, paramValue);
      return this;
    }

    /**
     * return the number of deleted rows
     *
     * @param client
     * @return
     */
    public Integer execute(DatabaseClient client)
    {
      int index = -1;
      if ((keys==null || keys.isEmpty()) && (index = selectStatementHolder.query.indexOf(DYNAMIC))!=-1){
        keys = getKeys(client, tableName);
        if (keys==null || keys.isEmpty()){
          throw new RuntimeException("No keys found for table: " + tableName);
        }
        selectStatementHolder.query.replace(index, index+DYNAMIC.length(),join(keys));
      }
      return client.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Integer>()
      {
        @Nullable
        @Override
        public Integer run(TransactionContext transaction) throws Exception
        {
          return DeleteMutationHolder.this.execute(transaction);
        }
      });
    }

    public Integer execute(TransactionContext transaction)
    {
      int index = -1;
      if ((index = selectStatementHolder.query.indexOf(DYNAMIC))!=-1){
        throw new RuntimeException("Cannot dynamically fetch keys for query in a read write transaction. You must specify the keys!");
      }
      int count = 0;
      try ( ResultSet rs = selectStatementHolder.execute(transaction) )
      {
        while ( rs.next() )
        {
          Key.Builder keyBuilder = Key.newBuilder();
          for ( String key : keys )
          {
            setToBuilderFromResultSet(keyBuilder, rs, key);
          }
          transaction.buffer(Mutation.delete(tableName, keyBuilder.build()));
          count++;
        }
      }
      return count;
    }

    public String getTableName()
    {
      return tableName;
    }

    @Override
    public String toString()
    {
      return "DeleteMutationHolder{" +
        "query='" + query + '\'' +
        ", tableName='" + tableName + '\'' +
        ", keys=" + keys +
        ", selectStatementHolder=" + selectStatementHolder +
        '}';
    }
  }

}
