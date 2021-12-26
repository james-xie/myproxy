package com.gllue.myproxy.command.result.query;

import java.util.function.Predicate;

public class FilteredQueryResult extends QueryResultDecorator {

  /** Predicate to apply to each row to determine if it should be included */
  private final Predicate<QueryResult> predicate;

  public FilteredQueryResult(QueryResult queryResult, Predicate<QueryResult> predicate) {
    super(queryResult);
    this.predicate = predicate;
  }

  @Override
  public boolean next() {
    boolean hasNext = super.next();
    while (hasNext) {
      if (predicate.test(this)) {
        return true;
      }
      hasNext = super.next();
    }
    return false;
  }
}
