

package com.excellenceengineeringsolutions.spannerjdbc;

import com.excellenceengineeringsolutions.AppException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.TransactionContext;

/**
 * binds and executes the Spanner query/mutation
 */
public interface SpannerMutationStatement
{
  /**
   * bind a parameter by index
   *
   * @param paramNo
   * @param paramValue
   */
  SpannerMutationStatement bind(int paramNo, Object paramValue);

  /**
   * execute query mutation directly with the client
   * @param client
   */
  void execute(DatabaseClient client) throws AppException;

  /**
   * execute query/mutation using the transaction context
   * @param transaction
   */
  void execute(TransactionContext transaction) throws AppException;

}
