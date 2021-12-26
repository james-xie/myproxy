package com.gllue.myproxy.command.result.query;

public class SingleRowQueryResult extends DefaultQueryResult {
  public SingleRowQueryResult(final QueryResultMetaData queryResultMetaData, final String[] row) {
    super(queryResultMetaData, new String[][] {row});
  }
}
