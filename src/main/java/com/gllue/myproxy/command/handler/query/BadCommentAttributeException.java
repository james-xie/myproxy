package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BadCommentAttributeException extends CommandHandlerException {
  private final SQLCommentAttributeKey key;
  private final String value;

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BAD_COMMENT_ATTRIBUTE;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {key.name(), value};
  }
}
