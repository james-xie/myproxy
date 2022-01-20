package com.gllue.myproxy.transport.core.service;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BadDataSourceException extends BaseServerException {
  private final String dataSource;

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BAD_DATA_SOURCE;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {dataSource};
  }
}
