package com.excellenceengineeringsolutions.copydb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 *
 */
@Service
public class FromSpannerService extends SpannerService
{
  public FromSpannerService(
    @Autowired
    @Qualifier("FromDbProperties")
      SpannerProperties spannerProperties)
  {
    super(spannerProperties);
  }
}
