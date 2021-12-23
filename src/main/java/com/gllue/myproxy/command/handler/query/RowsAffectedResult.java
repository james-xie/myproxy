package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;

public interface RowsAffectedResult extends HandlerResult {
  long getAffectedRows();
}
