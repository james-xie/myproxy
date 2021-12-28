package com.gllue.myproxy.command.handler.query.dml.delete;

import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.command.handler.query.dml.update.UpdateQueryRewriteVisitor;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;

public class DeleteQueryHandler extends AbstractDMLHandler {
  private static final String NAME = "Delete query handler";

  public DeleteQueryHandler(
      PersistRepository repository,
      Configurations configurations,
      ClusterState clusterState,
      TransportService transportService) {
    super(repository, configurations, clusterState, transportService);
  }

  @Override
  public String name() {
    return NAME;
  }

  private DeleteQueryRewriteVisitor newQueryRewriteVisitor(QueryHandlerRequest request) {
    return new DeleteQueryRewriteVisitor(request.getDatabase(), newScopeFactory(request));
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
