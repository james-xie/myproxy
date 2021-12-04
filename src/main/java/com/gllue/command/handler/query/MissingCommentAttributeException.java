package com.gllue.command.handler.query;

import com.gllue.common.exception.BaseServerException;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MissingCommentAttributeException extends BaseServerException {
  private final SQLCommentAttributeKey key;

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_MISSING_COMMENT_ATTRIBUTE;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {key.name()};
  }
}
