package com.gllue.command.handler.query;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueryHandlerRequestImpl implements QueryHandlerRequest {
  private final int connectionId;
  private final String datasource;
  private final String database;
  private final String query;

  private SQLStatement statement;
  private Map<SQLCommentAttributeKey, Object> attributes;

  @Override
  public int getConnectionId() {
    return connectionId;
  }

  @Override
  public String getDatasource() {
    return datasource;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public void setStatement(SQLStatement stmt) {
    this.statement = stmt;
  }

  @Override
  public SQLStatement getStatement() {
    return statement;
  }

  @Override
  public void setCommentsAttributes(Map<SQLCommentAttributeKey, Object> attributes) {
    Preconditions.checkNotNull(attributes);
    if (this.attributes == null) {
      this.attributes = attributes;
    } else {
      throw new IllegalArgumentException("Cannot override attributes.");
    }
  }

  @Override
  public Map<SQLCommentAttributeKey, Object> getCommentsAttributes() {
    assert this.attributes != null;
    return this.attributes;
  }
}
