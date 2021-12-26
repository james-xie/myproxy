package com.gllue.myproxy.command.handler.query.dcl.set;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.BadEncryptKeyException;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;

public class SetStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Set statement handler";
  private static final String ENCRYPT_KEY = "ENCRYPT_KEY";

  public SetStatementHandler(TransportService transportService) {
    super(transportService);
  }

  @Override
  public String name() {
    return NAME;
  }

  private String encryptKeyPrefix(String database) {
    return database + ":";
  }

  /**
   * Set the encryption key to session context.
   *
   * <pre>
   * To ensure that the encryption key is not mixed with other sessions, the encryption key format must be as follows:
   *  "databaseName:encryptKey"
   * </pre>
   *
   * @param request handler request
   * @param value the assignment value
   */
  private void processEncryptKey(QueryHandlerRequest request, SQLExpr value) {
    if (value instanceof SQLNullExpr) {
      request.getSessionContext().setEncryptKey(null);
      return;
    }

    var database = request.getDatabase();
    if (database == null) {
      throw new NoDatabaseException();
    }

    String encryptKey = null;
    if (value instanceof SQLCharExpr) {
      encryptKey = ((SQLCharExpr) value).getText();
      var prefix = encryptKeyPrefix(database);
      if (encryptKey.startsWith(prefix)) {
        if (encryptKey.length() > prefix.length()) {
          encryptKey = encryptKey.substring(prefix.length());
        } else {
          encryptKey = null;
        }
      } else {
        encryptKey = null;
      }
    }

    if (encryptKey == null) {
      throw new BadEncryptKeyException(value.toString());
    }

    request.getSessionContext().setEncryptKey(encryptKey);
  }

  private boolean processAssignItem(QueryHandlerRequest request, SQLAssignItem item) {
    if (ENCRYPT_KEY.equalsIgnoreCase(item.getTarget().toString())) {
      processEncryptKey(request, item.getValue());
      return true;
    }
    return false;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLSetStatement) request.getStatement();
    var newItems = new ArrayList<SQLAssignItem>();
    for (var item : stmt.getItems()) {
      if (!processAssignItem(request, item)) {
        newItems.add(item);
      }
    }
    if (newItems.isEmpty()) {
      callback.onSuccess(QueryHandlerResult.OK_RESULT);
      return;
    }

    submitQueryToBackendDatabase(
        request.getConnectionId(),
        request.getQuery(),
        WrappedHandlerResult.wrappedCallback(callback));
  }
}
