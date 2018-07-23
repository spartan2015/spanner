

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.DatabaseClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds default fields values for insertion into the database
 */
public class StatementDefaults
{

  private final Map<String, Map<String, Object>> defaultParameters;

  {
    defaultParameters = new HashMap<>();

  }

  public StatementDefaults(DatabaseClient client)
  {

  }

  public Map<String, Map<String, Object>> getDefaultParameters()
  {
    return defaultParameters;
  }


}
