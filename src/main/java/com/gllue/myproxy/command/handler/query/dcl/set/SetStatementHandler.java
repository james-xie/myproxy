package com.gllue.myproxy.command.handler.query.dcl.set;

import static com.gllue.myproxy.common.util.SQLStatementUtils.newSQLSetStatement;
import static com.gllue.myproxy.common.util.SQLStatementUtils.toSQLString;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.BadEncryptKeyException;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class SetStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Set statement handler";
  private static final String ENCRYPT_KEY = "@ENCRYPT_KEY";
  private static final String AUTOCOMMIT = "AUTOCOMMIT";

  private static final Set<String> AUTOCOMMIT_ENABLE_VALUES = Set.of("ON", "1", "TRUE");
  private static final Set<String> AUTOCOMMIT_DISABLE_VALUES = Set.of("OFF", "0", "FALSE");

  public SetStatementHandler(final TransportService transportService, final ThreadPool threadPool) {
    super(transportService, threadPool);
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

  private Promise<CommandResult> processAutoCommit(
      QueryHandlerRequest request, SQLAssignItem item) {
    var valStr = item.getValue().toString().toUpperCase();
    if (AUTOCOMMIT_ENABLE_VALUES.contains(valStr)) {
      return setAutoCommit(request.getConnectionId(), true);
    } else if (AUTOCOMMIT_DISABLE_VALUES.contains(valStr)) {
      return setAutoCommit(request.getConnectionId(), false);
    } else {
      var targetStr = item.getTarget().toString();
      throw new BadVariableValueException(targetStr, valStr);
    }
  }

  private boolean processAssignItem(
      QueryHandlerRequest request,
      SQLAssignItem item,
      List<Supplier<Promise<CommandResult>>> processors) {
    var targetStr = item.getTarget().toString();
    if (ENCRYPT_KEY.equalsIgnoreCase(targetStr)) {
      processEncryptKey(request, item.getValue());
      return true;
    }
    if (AUTOCOMMIT.equalsIgnoreCase(targetStr)) {
      var target = (SQLVariantRefExpr) item.getTarget();
      if (!target.isGlobal()) {
        processors.add(() -> processAutoCommit(request, item));
        return true;
      }
    }
    return false;
  }

  private Function<CommandResult, Promise<CommandResult>> processorSupplier(
      List<Supplier<Promise<CommandResult>>> delayProcessors) {
    var size = delayProcessors.size();
    var index = new AtomicInteger(0);
    return (result) -> {
      var i = index.getAndIncrement();
      if (i >= size) return null;
      return delayProcessors.get(i).get();
    };
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLSetStatement) request.getStatement();
    var afterProcessors = new ArrayList<Supplier<Promise<CommandResult>>>();
    var remainItems = new ArrayList<SQLAssignItem>();
    for (var item : stmt.getItems()) {
      var processed = processAssignItem(request, item, afterProcessors);
      if (!processed) {
        remainItems.add(item);
      }
    }

    if (remainItems.isEmpty() && afterProcessors.isEmpty()) {
      callback.onSuccess(QueryHandlerResult.OK_RESULT);
      return;
    }

    Promise<CommandResult> promise;
    if (!remainItems.isEmpty()) {
      var connectionId = request.getConnectionId();
      var newQuery = toSQLString(newSQLSetStatement(remainItems));
      promise = new Promise<>((cb) -> submitQueryToBackendDatabase(connectionId, newQuery, cb));
    } else {
      promise = Promise.emptyPromise();
    }

    if (!afterProcessors.isEmpty()) {
      promise = promise.thenAsync((v) -> Promise.chain(processorSupplier(afterProcessors)));
    }

    promise.then(
        (result) -> {
          callback.onSuccess(new WrappedHandlerResult(result));
          return true;
        },
        (e) -> {
          callback.onFailure(e);
          return false;
        });
  }
}
