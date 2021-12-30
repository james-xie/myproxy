package com.gllue.myproxy.command.handler.query.dml.insert;

import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.stream.Collectors;

public class InsertQueryHandler extends AbstractDMLHandler {
  private static final String NAME = "Insert query handler";

  private final IdGenerator idGenerator;

  public InsertQueryHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final IdGenerator idGenerator,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, threadPool);
    this.idGenerator = idGenerator;
  }

  @Override
  public String name() {
    return NAME;
  }

  private InsertQueryRewriteVisitor newQueryRewriteVisitor(QueryHandlerRequest request) {
    String encryptKey = request.getSessionContext().getEncryptKey();
    return new InsertQueryRewriteVisitor(
        request.getDatabase(),
        newScopeFactory(request),
        request.getDatasource(),
        clusterState.getMetaData(),
        idGenerator,
        newEncryptor(encryptKey),
        newDecryptor(encryptKey));
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    ensureDatabaseExists(request);

    var stmt = (SQLInsertStatement) request.getStatement();
    var visitor = newQueryRewriteVisitor(request);
    stmt.accept(visitor);
    if (!visitor.isQueryChanged()) {
      submitQueryToBackendDatabase(
          request.getConnectionId(),
          request.getQuery(),
          WrappedHandlerResult.wrappedCallback(callback));
      return;
    }

    var newInsertStmts = visitor.getNewInsertQueries();
    if (newInsertStmts == null || newInsertStmts.isEmpty()) {
      var newSql = SQLStatementUtils.toSQLString(stmt);
      submitQueryToBackendDatabase(
          request.getConnectionId(), newSql, WrappedHandlerResult.wrappedCallback(callback));
      return;
    }

    var newInsertQueries =
        newInsertStmts.stream().map(SQLStatementUtils::toSQLString).collect(Collectors.toList());
    var promise = executeQueriesAtomically(request, newInsertQueries);
    promise
        .then(
            (result) -> {
              var lastResult = result.get(result.size() - 1);
              callback.onSuccess(new WrappedHandlerResult(lastResult));
              return true;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
