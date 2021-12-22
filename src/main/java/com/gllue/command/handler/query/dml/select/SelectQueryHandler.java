package com.gllue.command.handler.query.dml.select;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.gllue.cluster.ClusterState;
import com.gllue.command.handler.query.QueryHandlerRequest;
import com.gllue.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.config.Configurations;
import com.gllue.repository.PersistRepository;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.core.service.TransportService;

public class SelectQueryHandler extends AbstractDMLHandler<SelectQueryResult> {
  private static final String NAME = "Select query handler";

  public SelectQueryHandler(
      PersistRepository repository,
      Configurations configurations,
      ClusterState clusterState,
      TransportService transportService,
      SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  @Override
  public String name() {
    return NAME;
  }

  private SelectQueryRewriteVisitor newQueryRewriteVisitor(QueryHandlerRequest request) {
    String encryptKey = null;
    return new SelectQueryRewriteVisitor(
        request.getDatabase(), newScopeFactory(request), encryptKey);
  }

  private Callback<CommandResult> directTransferResultCallback(
      Callback<SelectQueryResult> callback) {
    return new Callback<CommandResult>() {
      @Override
      public void onSuccess(CommandResult result) {
        callback.onSuccess(SelectQueryResult.DIRECT_TRANSFERRED_RESULT);
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onSuccess(SelectQueryResult.DIRECT_TRANSFERRED_RESULT);
      }
    };
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<SelectQueryResult> callback) {
    var stmt = (SQLSelectStatement) request.getStatement();
    if (stmt.getSelect().getFirstQueryBlock().getFrom() == null) {
      submitQueryAndDirectTransferResult(
          request.getConnectionId(), request.getQuery(), directTransferResultCallback(callback));
      return;
    }

    ensureDatabaseExists(request);

    var visitor = newQueryRewriteVisitor(request);
    stmt.accept(visitor);
    if (!visitor.isQueryChanged()) {
      submitQueryAndDirectTransferResult(
          request.getConnectionId(), request.getQuery(), directTransferResultCallback(callback));
      return;
    }

    var newSql = SQLStatementUtils.toSQLString(stmt);
    submitQueryAndDirectTransferResult(
        request.getConnectionId(), newSql, directTransferResultCallback(callback));
  }
}
