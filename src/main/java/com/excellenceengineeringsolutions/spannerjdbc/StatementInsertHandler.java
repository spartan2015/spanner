

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionContext;
import com.excellenceengineeringsolutions.spanner.SpannerMutationStatement;
import com.excellenceengineeringsolutions.db.intf.SdfException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.TABLE_NAME_REG_EXP;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.bindParam;
import static com.excellenceengineeringsolutions.spanner.StatementHandlerCommon.parseValue;


/**
 * Handles executions of sql queries on spanner
 */
public class StatementInsertHandler
{
  private static final String colGroup = "\\s*\\(([^)]+)\\)";
  private static final String valGroup = "\\s*values\\s*\\((.+)\\)";
  private static final Pattern INSERT_INTO_REGEXP = Pattern.compile("(?i)INSERT[ ]+INTO[ ]+(" + TABLE_NAME_REG_EXP + ")" + colGroup + valGroup);

  // force non-instantiability through the `private` constructor
  private StatementInsertHandler()
  {
    throw new AssertionError("This class cannot be instantiated");
    // or, throw `UnsupportedOperationException`
  }

  public static InsertMutationHolder spannerInsertBuilder(String insert, Map<String, Map<String, Object>> defaultParameters) throws SdfException
  {
    String[] insertParts = parserInsert(insert);
    Mutation.WriteBuilder writer = Mutation.newInsertBuilder(insertParts[0]);
    Map<String, Object> paramsInInsert = getParamsFromInsert(insertParts[1], insertParts[2]);
    Map<String, String> requiredParams = new HashMap<>();
    for ( String param : paramsInInsert.keySet() )
    {
      Object paramValue = paramsInInsert.get(param);
      if ( !(paramValue instanceof String && ((String) paramValue).startsWith("@_")) )
      {
        bindParam(writer, param.trim(), paramValue);
      } else
      {
        requiredParams.put(((String) paramValue).trim(), param);
      }
    }
    return new InsertMutationHolder(insert, insertParts[0], writer, requiredParams, paramsInInsert, defaultParameters);
  }

  public static Map<String, Object> getParamsFromInsert(String columnsGroup, String valueGroup)
  {
    Map<String, Object> result = new HashMap<>();
    String[] columns = columnsGroup.split(",");
    Object[] values = parseValueGroup(valueGroup);
    int colIndex = 1;
    for ( int i = 0; i < columns.length; i++ )
    {
      result.put(columns[i].trim().toUpperCase(), ("?".equals(values[i])) ? "@_" + (colIndex++) : values[i]);
    }
    return result;
  }

  static Object[] parseValueGroup(String valueGroup)
  {
    boolean inString = false;
    List<Object> result = new ArrayList<>();
    char prevChar = 0;
    for ( int from = 0, i = 0; i < valueGroup.length(); i++ )
    {
      char currentChar = valueGroup.charAt(i);
      if ( currentChar == '\'' && prevChar != '\\' )
      {
        inString = !inString;
      }
      boolean atEnd = i == valueGroup.length() - 1;
      if ( (currentChar == ',' && !inString) || atEnd )
      {
        String value = valueGroup.substring(from, atEnd ? i + 1 : i).trim();

        result.add(parseValue(value));
        from = i + 1;
      }
      prevChar = currentChar;
    }

    return result.toArray();
  }

  static String[] parserInsert(String insert) throws SdfException
  {
    Matcher matcher = INSERT_INTO_REGEXP.matcher(insert);
    if ( matcher.find() )
    {
      return new String[]{matcher.group(1), matcher.group(2), matcher.group(3)};
    } else
    {
      throw new SdfException(String.format("Could not extract table name from query [%s]", insert));
    }
  }

  public static class InsertMutationHolder implements com.excellenceengineeringsolutions.spanner.SpannerMutationStatement
  {
    String query;
    String tableName;
    Mutation.WriteBuilder mutationBuilder;
    Map<String, String> requiredParams;
    Map<String, Object> paramsInInsert;
    Map<String, Map<String, Object>> defaultParameters;

    public InsertMutationHolder(String query, String tableName, Mutation.WriteBuilder mutationBuilder, Map<String, String> requiredParams,
                                Map<String, Object> paramsInInsert, Map<String, Map<String, Object>> defaultParameters)
    {
      this.query = query;
      this.tableName = tableName;
      this.mutationBuilder = mutationBuilder;
      this.requiredParams = requiredParams;
      this.paramsInInsert = paramsInInsert;
      this.defaultParameters = defaultParameters;
    }

    public void bind(int paramNo, Object paramValue, Class<?> clazz)
    {
      String key = "@_" + paramNo;
      if ( paramValue != null )
      {
        bindParam(mutationBuilder, requiredParams.get(key), paramValue, clazz);
      }
      requiredParams.remove(key);
    }

    public SpannerMutationStatement bind(int paramNo, Object paramValue)
    {
      String key = "@_" + paramNo;
      if ( paramValue != null )
      {
        bindParam(mutationBuilder, requiredParams.get(key), paramValue);
      }
      requiredParams.remove(key);
      return this;
    }

    public void execute(DatabaseClient client) throws SdfException
    {
      client.write(Arrays.asList(build()));
    }

    public void execute(TransactionContext transaction) throws SdfException
    {
      transaction.buffer(build());
    }

    public Mutation build() throws SdfException
    {
      if ( !requiredParams.isEmpty() )
      {
        throw new SdfException("missing required params: " + requiredParams + " in original query: " + query);
      }
      for ( String defaultParam : defaultParameters.get(tableName).keySet() )
      {
        if ( !paramsInInsert.containsKey(defaultParam) )
        {
          Object value = getInsertParamValue(defaultParam);
          paramsInInsert.put(defaultParam, value);
          bindParam(mutationBuilder, defaultParam, value);
        }
      }
      return mutationBuilder.build();
    }

    private Object getInsertParamValue(String defaultParam) throws SdfException
    {
      Object paramValue = defaultParameters.get(tableName).get(defaultParam);
      if ( paramValue instanceof Callable )
      {
        try
        {
          paramValue = ((Callable) paramValue).call();
        }
        catch ( Exception e )
        {
          throw new SdfException("Could not lazy fetch parameter", e);
        }
      }
      return paramValue;
    }

    @Override
    public String toString()
    {
      return "InsertMutationHolder{" +
        "query='" + query + '\'' +
        ", tableName='" + tableName + '\'' +
        ", requiredParams=" + requiredParams +
        ", paramsInInsert=" + paramsInInsert +
        '}';
    }
  }
}