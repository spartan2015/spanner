

package com.excellenceengineeringsolutions.spannerjdbc;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds default fields values for insertion into the database
 */
public class StatementDefaults
{

  private final Map<String, Map<String, Object>> defaultParameters;
  private volatile SpannerClientProvider spannerClientProvider;
  private volatile SpannerSequenceGenerator spannerSequenceGenerator;

  {
    defaultParameters = new HashMap<>();

  }

  public StatementDefaults(SpannerClientProvider spannerClientProvider, SpannerSequenceGenerator spannerSequenceGenerator)
  {
    this.spannerClientProvider = spannerClientProvider;
    this.spannerSequenceGenerator = spannerSequenceGenerator;
  }

  public long getNextSequenceValue(String sequenceName)
  {
    return BitwiseReverser.reverseBits(spannerSequenceGenerator.getNextValue(sequenceName));
  }

  public Map<String, Map<String, Object>> getDefaultParameters()
  {
    return defaultParameters;
  }


}
