package com.gllue.myproxy.command.handler.query.dml.delete;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.EncryptionHelper;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;

public class DeleteQueryHandler extends AbstractDMLHandler {
  private static final String NAME = "Delete query handler";

  public DeleteQueryHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  private DeleteQueryRewriteVisitor newQueryRewriteVisitor(QueryHandlerRequest request) {
    var encryptKey = EncryptionHelper.getEncryptKey(request);
    return new DeleteQueryRewriteVisitor(
        request.getDatabase(), newScopeFactory(request), newEncryptor(encryptKey));
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    ensureDatabaseExists(request);

    var stmt = request.getStatement();
    var visitor = newQueryRewriteVisitor(request);
    stmt.accept(visitor);
    if (!visitor.isQueryChanged()) {
      submitQueryToBackendDatabase(
          request.getConnectionId(),
          request.getQuery(),
          WrappedHandlerResult.wrappedCallback(callback));
      return;
    }

    var newSql = SQLStatementUtils.toSQLString(stmt);
    submitQueryToBackendDatabase(
        request.getConnectionId(), newSql, WrappedHandlerResult.wrappedCallback(callback));
  }
}
