

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.excellenceengineeringsolutions.spannerjdbc.StatementHandlerCommon.spannerQueryParams;

/**
 * Handle sql select statements through Spanner Client
 */
public class StatementSelectHandler
{


  // force non-instantiability through the `private` constructor
  private StatementSelectHandler()
  {
    throw new AssertionError("This class cannot be instantiated");
    // or, throw `UnsupportedOperationException`
  }

  public static SelectStatementHolder spannerQueryBuilder(String select)
  {
    return spannerQueryBuilder(select, new AtomicInteger(1));
  }

  public static SelectStatementHolder spannerQueryBuilder(String select, AtomicInteger index)
  {
    return new SelectStatementHolder(spannerQueryParams(select, index));
  }

  public static class SelectStatementHolder
  {
    StringBuilder query;
    Map<String, Object> bind = new HashMap();

    public SelectStatementHolder(String query)
    {
      this.query = new StringBuilder(query);
    }

    public SelectStatementHolder bind(int paramIndex, Object paramValue)
    {
      if ( paramValue == null )
      {
        replaceWithIsNull("_" + paramIndex);
      } else
      {
        bind.put("_" + paramIndex, paramValue);
      }
      return this;
    }

    public SelectStatementHolder bind(String paramIndex, Object paramValue)
    {
      if ( paramValue == null )
      {
        replaceWithIsNull("_" + paramIndex);
      } else
      {
        bind.put(paramIndex, paramValue);
      }
      return this;
    }

    public ResultSet execute(TransactionContext ctx)
    {
      return ctx.executeQuery(build());
    }

    public ResultSet execute(DatabaseClient client)
    {
      return client.singleUse().executeQuery(build());
    }

    Statement build()
    {
      Statement.Builder builder = Statement.newBuilder(query.toString());
      for ( String param : bind.keySet() )
      {
        StatementHandlerCommon.bind(builder, param, bind.get(param));
      }
      return builder.build();
    }

    private void replaceWithIsNull(String key)
    {
      int index = query.indexOf(key);
      int startsFrom = index - 1;
      while ( query.charAt(startsFrom) != '=' )
      {
        startsFrom--;
      }
      query = query.delete(startsFrom, index + key.length()).insert(startsFrom, " is null");
    }

    @Override
    public String toString()
    {
      return "SelectStatementHolder{" +
        "query='" + query.toString() + '\'' +
        '}';
    }
  }



}
