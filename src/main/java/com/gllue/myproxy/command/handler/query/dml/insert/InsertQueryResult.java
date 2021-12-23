package com.gllue.myproxy.command.handler.query.dml.insert;

import com.gllue.myproxy.command.handler.query.RowsAffectedResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class InsertQueryResult implements RowsAffectedResult {
  private final long affectedRows;
  private final long lastInsertId;
  private final long warnings;

  @Override
  public boolean isDirectTransferred() {
    return false;
  }
}
