package com.gllue.command.handler.query;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.gllue.command.handler.HandlerRequest;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import java.util.Map;

public interface QueryHandlerRequest extends HandlerRequest {
  String getQuery();

  void setStatement(SQLStatement stmt);

  SQLStatement getStatement();

  void setCommentsAttributes(Map<SQLCommentAttributeKey, Object> attributes);

  Map<SQLCommentAttributeKey, Object> getCommentsAttributes();
}
