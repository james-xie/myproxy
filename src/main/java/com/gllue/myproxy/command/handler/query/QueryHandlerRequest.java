package com.gllue.myproxy.command.handler.query;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.gllue.myproxy.command.handler.HandlerRequest;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import com.gllue.myproxy.transport.frontend.connection.SessionContext;
import java.util.Map;

public interface QueryHandlerRequest extends HandlerRequest {
  String getQuery();

  void setStatement(SQLStatement stmt);

  SQLStatement getStatement();

  void setCommentsAttributes(Map<SQLCommentAttributeKey, Object> attributes);

  Map<SQLCommentAttributeKey, Object> getCommentsAttributes();

  SessionContext getSessionContext();
}
