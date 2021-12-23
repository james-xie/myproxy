package com.gllue.myproxy.command.handler.query;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.CommandHandler;
import com.gllue.myproxy.command.handler.HandlerRequest;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.dcl.set.SetHandler;
import com.gllue.myproxy.command.handler.query.ddl.alter.AlterTableHandler;
import com.gllue.myproxy.command.handler.query.ddl.create.CreateTableHandler;
import com.gllue.myproxy.command.handler.query.dml.insert.InsertQueryHandler;
import com.gllue.myproxy.command.handler.query.dml.select.SelectQueryHandler;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;

public class ConcreteQueryHandler extends AbstractQueryHandler<HandlerResult> {
  private static final String NAME = "Concrete Query Handler";

  private final AlterTableHandler alterTableHandler;
  private final CreateTableHandler createTableHandler;
  private final SelectQueryHandler selectQueryHandler;
  private final InsertQueryHandler insertQueryHandler;

  private final SetHandler setHandler;

  public ConcreteQueryHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser,
      final IdGenerator idGenerator) {
    super(repository, configurations, clusterState, transportService, sqlParser);

    // Init query handlers.
    this.alterTableHandler =
        new AlterTableHandler(
            repository, configurations, clusterState, transportService, sqlParser);
    this.createTableHandler =
        new CreateTableHandler(
            repository, configurations, clusterState, transportService, sqlParser);
    this.selectQueryHandler =
        new SelectQueryHandler(
            repository, configurations, clusterState, transportService, sqlParser);
    this.insertQueryHandler =
        new InsertQueryHandler(
            repository, configurations, clusterState, transportService, sqlParser, idGenerator);

    this.setHandler =
        new SetHandler(repository, configurations, clusterState, transportService, sqlParser);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = sqlParser.parse(request.getQuery());
    var comments = stmt.getAfterCommentsDirect();
    var attributes = sqlParser.parseComments(comments);
    request.setStatement(stmt);
    request.setCommentsAttributes(attributes);

    dispatchQueryHandler(stmt, request, callback);
  }

  private <Request extends HandlerRequest, Result extends HandlerResult> void invokeHandlerExecute(
      CommandHandler<Request, Result> handler, Request request, Callback<Result> callback) {
    handler.execute(request, callback);
  }

  private Callback<CommandResult> directTransferResultCallback(Callback<HandlerResult> callback) {
    return new Callback<CommandResult>() {
      @Override
      public void onSuccess(CommandResult result) {
        callback.onSuccess(DirectTransferredResult.INSTANCE);
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onSuccess(DirectTransferredResult.INSTANCE);
      }
    };
  }

  private void dispatchQueryHandler(
      SQLStatement stmt, QueryHandlerRequest request, Callback<HandlerResult> callback) {
    if (stmt instanceof SQLSelectStatement) {
      invokeHandlerExecute(selectQueryHandler, request, Callback.wrap(callback));
    } else if (stmt instanceof MySqlUpdateStatement) {

    } else if (stmt instanceof MySqlInsertStatement) {
      invokeHandlerExecute(insertQueryHandler, request, Callback.wrap(callback));
    } else if (stmt instanceof MySqlDeleteStatement) {

    } else if (stmt instanceof SQLAlterTableStatement) {
      invokeHandlerExecute(alterTableHandler, request, Callback.wrap(callback));
    } else if (stmt instanceof MySqlCreateTableStatement) {
      invokeHandlerExecute(createTableHandler, request, Callback.wrap(callback));
    } else if (stmt instanceof SQLDropTableStatement) {

    } else if (stmt instanceof MySqlRenameTableStatement) {

    } else if (stmt instanceof SQLSetStatement) {
      invokeHandlerExecute(setHandler, request, Callback.wrap(callback));
    } else {
      submitQueryAndDirectTransferResult(
          request.getConnectionId(), request.getQuery(), directTransferResultCallback(callback));
    }
  }
}
