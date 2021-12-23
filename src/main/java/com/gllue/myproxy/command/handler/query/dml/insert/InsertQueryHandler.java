package com.gllue.myproxy.command.handler.query.dml.insert;

import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.stream.Collectors;

public class InsertQueryHandler extends AbstractDMLHandler<InsertQueryResult> {
  private static final String NAME = "Insert query handler";

  private final IdGenerator idGenerator;

  public InsertQueryHandler(
      PersistRepository repository,
      Configurations configurations,
      ClusterState clusterState,
      TransportService transportService,
      SQLParser sqlParser,
      IdGenerator idGenerator) {
    super(repository, configurations, clusterState, transportService, sqlParser);
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
        encryptKey);
  }

  private Callback<CommandResult> newCallback(Callback<InsertQueryResult> callback) {
    return new Callback<>() {
      @Override
      public void onSuccess(CommandResult result) {
        callback.onSuccess(
            new InsertQueryResult(
                result.getAffectedRows(), result.getLastInsertId(), result.getWarnings()));
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onFailure(e);
      }
    };
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<InsertQueryResult> callback) {
    ensureDatabaseExists(request);

    var stmt = (SQLInsertStatement) request.getStatement();
    var visitor = newQueryRewriteVisitor(request);
    stmt.accept(visitor);
    if (!visitor.isQueryChanged()) {
      submitQueryToBackendDatabase(
          request.getConnectionId(), request.getQuery(), newCallback(callback));
      return;
    }

    var newInsertStmts = visitor.getNewInsertQueries();
    if (newInsertStmts == null) {
      var newSql = SQLStatementUtils.toSQLString(stmt);
      submitQueryToBackendDatabase(request.getConnectionId(), newSql, newCallback(callback));
      return;
    }

    var newInsertQueries =
        newInsertStmts.stream().map(SQLStatementUtils::toSQLString).collect(Collectors.toList());
    var promise = executeQueriesAtomically(request, newInsertQueries);
    promise.then(
        (result) -> {
          callback.onSuccess(
              new InsertQueryResult(
                  result.getAffectedRows(), result.getLastInsertId(), result.getWarnings()));
          return true;
        },
        (e) -> {
          callback.onFailure(e);
          return false;
        });
  }
}
