package com.excellenceengineeringsolutions.copydb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 *
 */
@Service
public class ToSpannerService extends SpannerService
{

  public ToSpannerService(
    @Autowired
    @Qualifier("ToDbProperties")
      SpannerProperties spannerProperties)
  {
    super(spannerProperties);
  }
}
