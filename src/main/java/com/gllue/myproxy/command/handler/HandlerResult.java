package com.gllue.myproxy.command.handler;

import com.gllue.myproxy.command.result.query.QueryResult;

public interface HandlerResult {
  long getAffectedRows();

  long getLastInsertId();

  int getWarnings();

  QueryResult getQueryResult();

  boolean isDirectTransferred();
}
