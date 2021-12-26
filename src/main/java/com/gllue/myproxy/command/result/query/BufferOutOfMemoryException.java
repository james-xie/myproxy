package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;

public class BufferOutOfMemoryException extends BaseServerException {
  private final int sizeInKB;

  public BufferOutOfMemoryException(final int sizeInBytes) {
    this.sizeInKB = sizeInBytes / 1024;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BUFFER_OUT_OF_MEMORY;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {String.valueOf(sizeInKB)};
  }
}
