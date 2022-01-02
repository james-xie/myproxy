package com.gllue.myproxy.command.handler.query.ddl.drop;

import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.util.SQLErrorUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.DropDatabaseCommand;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;

public class DropDatabaseHandler extends AbstractDDLHandler {
  private static final String NAME = "Drop database handler";

  public DropDatabaseHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, sqlParser, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLDropDatabaseStatement) request.getStatement();
    var databaseName = unquoteName(stmt.getDatabaseName());
    submitQueryToBackendDatabase(request.getConnectionId(), request.getQuery())
        .then(
            (result) -> {
              var command = new DropDatabaseCommand(request.getDatasource(), databaseName);
              command.execute(newCommandExecutionContext());
              callback.onSuccess(new WrappedHandlerResult(result));
              return true;
            })
        .doCatch(
            (e) -> {
              if (SQLErrorUtils.isBadDB(e)) {
                var command = new DropDatabaseCommand(request.getDatasource(), databaseName);
                command.execute(newCommandExecutionContext());
              }
              callback.onFailure(e);
              return false;
            });
  }
}
