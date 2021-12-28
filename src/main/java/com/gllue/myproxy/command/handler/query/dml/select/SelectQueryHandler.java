package com.gllue.myproxy.command.handler.query.dml.select;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.EncryptionHelper;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;

public class SelectQueryHandler extends AbstractDMLHandler {
  private static final String NAME = "Select query handler";

  public SelectQueryHandler(
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

  private SelectQueryRewriteVisitor newQueryRewriteVisitor(QueryHandlerRequest request) {
    String encryptKey = EncryptionHelper.getEncryptKey(request);
    return new SelectQueryRewriteVisitor(
        request.getDatabase(), newScopeFactory(request), newDecryptor(encryptKey));
  }

  private boolean isSimpleSelectQuery(SQLSelectStatement stmt) {
    // fast path.
    if (stmt.getSelect().getFirstQueryBlock().getFrom() != null) {
      return false;
    }

    var visitor = new CheckTableSourceVisitor();
    stmt.accept(visitor);
    return !visitor.hasTableSource();
  }

  private void handleSimpleSelectQuery(
      QueryHandlerRequest request, Callback<HandlerResult> callback) {
    submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLSelectStatement) request.getStatement();

    if (isSimpleSelectQuery(stmt)) {
      handleSimpleSelectQuery(request, callback);
      return;
    }

    ensureDatabaseExists(request);

    var visitor = newQueryRewriteVisitor(request);
    stmt.accept(visitor);
    if (!visitor.isQueryChanged()) {
      submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
      return;
    }

    var newSql = SQLStatementUtils.toSQLString(stmt);
    submitQueryAndDirectTransferResult(request.getConnectionId(), newSql, callback);
  }
}
