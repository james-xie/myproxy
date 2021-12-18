package com.gllue.myproxy.transport.exception;

public class MalformedPacketException extends TransportException {
  public MalformedPacketException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_MALFORMED_PACKET;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
