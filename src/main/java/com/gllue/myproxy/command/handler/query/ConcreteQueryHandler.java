package com.gllue.myproxy.command.handler.query;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDescribeStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.CommandHandler;
import com.gllue.myproxy.command.handler.HandlerRequest;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.dcl.kill.KillStatementHandler;
import com.gllue.myproxy.command.handler.query.dcl.set.SetStatementHandler;
import com.gllue.myproxy.command.handler.query.dcl.show.ShowCreateTableHandler;
import com.gllue.myproxy.command.handler.query.dcl.show.ShowMetricsHandler;
import com.gllue.myproxy.command.handler.query.dcl.show.ShowTablesHandler;
import com.gllue.myproxy.command.handler.query.ddl.alter.AlterTableHandler;
import com.gllue.myproxy.command.handler.query.ddl.create.CreateDatabaseHandler;
import com.gllue.myproxy.command.handler.query.ddl.create.CreateTableHandler;
import com.gllue.myproxy.command.handler.query.ddl.drop.DropDatabaseHandler;
import com.gllue.myproxy.command.handler.query.ddl.drop.DropTableHandler;
import com.gllue.myproxy.command.handler.query.ddl.rename.RenameTableHandler;
import com.gllue.myproxy.command.handler.query.ddl.truncate.TruncateTableHandler;
import com.gllue.myproxy.command.handler.query.dml.delete.DeleteQueryHandler;
import com.gllue.myproxy.command.handler.query.dml.insert.InsertQueryHandler;
import com.gllue.myproxy.command.handler.query.dml.select.SelectQueryHandler;
import com.gllue.myproxy.command.handler.query.dml.update.UpdateQueryHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.sql.stmt.SQLShowMetricsStatement;
import com.gllue.myproxy.transport.core.service.TransportService;

public class ConcreteQueryHandler extends SchemaRelatedQueryHandler {
  private static final String NAME = "Concrete Query Handler";

  private final SQLParser sqlParser;

  private final AlterTableHandler alterTableHandler;
  private final CreateTableHandler createTableHandler;
  private final SelectQueryHandler selectQueryHandler;
  private final InsertQueryHandler insertQueryHandler;
  private final UpdateQueryHandler updateQueryHandler;
  private final DeleteQueryHandler deleteQueryHandler;
  private final DropTableHandler dropTableHandler;
  private final TruncateTableHandler truncateTableHandler;
  private final CreateDatabaseHandler createDatabaseHandler;
  private final DropDatabaseHandler dropDatabaseHandler;
  private final RenameTableHandler renameTableHandler;

  private final SetStatementHandler setStatementHandler;
  private final ShowTablesHandler showTablesHandler;
  private final ShowCreateTableHandler showCreateTableHandler;
  private final KillStatementHandler killStatementHandler;

  // custom statement handlers.
  private final ShowMetricsHandler showMetricsHandler;

  public ConcreteQueryHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser,
      final IdGenerator idGenerator,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, threadPool);
    this.sqlParser = sqlParser;

    // Init query handlers.
    this.alterTableHandler =
        new AlterTableHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);
    this.createTableHandler =
        new CreateTableHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);
    this.selectQueryHandler =
        new SelectQueryHandler(
            repository, configurations, clusterState, transportService, threadPool);
    this.insertQueryHandler =
        new InsertQueryHandler(
            repository, configurations, clusterState, transportService, idGenerator, threadPool);
    this.updateQueryHandler =
        new UpdateQueryHandler(
            repository, configurations, clusterState, transportService, threadPool);
    this.deleteQueryHandler =
        new DeleteQueryHandler(
            repository, configurations, clusterState, transportService, threadPool);
    this.dropTableHandler =
        new DropTableHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);
    this.truncateTableHandler =
        new TruncateTableHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);
    this.createDatabaseHandler = new CreateDatabaseHandler(transportService, threadPool);
    this.dropDatabaseHandler =
        new DropDatabaseHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);
    this.renameTableHandler =
        new RenameTableHandler(
            repository, configurations, clusterState, transportService, sqlParser, threadPool);

    this.setStatementHandler = new SetStatementHandler(transportService, threadPool);
    this.showTablesHandler = new ShowTablesHandler(transportService, clusterState, threadPool);
    this.showCreateTableHandler =
        new ShowCreateTableHandler(transportService, clusterState, sqlParser, threadPool);
    this.killStatementHandler = new KillStatementHandler(transportService, threadPool);

    this.showMetricsHandler = new ShowMetricsHandler(transportService, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var query = request.getQuery();
    var stmt = sqlParser.parse(query);
    var comments = stmt.getAfterCommentsDirect();
    var attributes = sqlParser.parseComments(comments);
    request.setStatement(stmt);
    request.setCommentsAttributes(attributes);

    dispatchQueryHandler(stmt, request, callback);
  }

  private <Request extends HandlerRequest> void invokeHandlerExecute(
      CommandHandler<Request> handler, Request request, Callback<HandlerResult> callback) {
    handler.execute(request, callback);
  }

  private void dispatchQueryHandler(
      SQLStatement stmt, QueryHandlerRequest request, Callback<HandlerResult> callback) {
    if (stmt instanceof SQLSelectStatement) {
      invokeHandlerExecute(selectQueryHandler, request, callback);
    } else if (stmt instanceof MySqlUpdateStatement) {
      invokeHandlerExecute(updateQueryHandler, request, callback);
    } else if (stmt instanceof MySqlInsertStatement) {
      invokeHandlerExecute(insertQueryHandler, request, callback);
    } else if (stmt instanceof MySqlDeleteStatement) {
      invokeHandlerExecute(deleteQueryHandler, request, callback);
    } else if (stmt instanceof SQLAlterTableStatement) {
      invokeHandlerExecute(alterTableHandler, request, callback);
    } else if (stmt instanceof MySqlCreateTableStatement) {
      invokeHandlerExecute(createTableHandler, request, callback);
    } else if (stmt instanceof SQLDropTableStatement) {
      invokeHandlerExecute(dropTableHandler, request, callback);
    } else if (stmt instanceof SQLTruncateStatement) {
      invokeHandlerExecute(truncateTableHandler, request, callback);
    } else if (stmt instanceof SQLCreateDatabaseStatement) {
      invokeHandlerExecute(createDatabaseHandler, request, callback);
    } else if (stmt instanceof SQLDropDatabaseStatement) {
      invokeHandlerExecute(dropDatabaseHandler, request, callback);
    } else if (stmt instanceof MySqlRenameTableStatement) {
      invokeHandlerExecute(renameTableHandler, request, callback);
    } else if (stmt instanceof SQLShowCreateTableStatement) {
      invokeHandlerExecute(showCreateTableHandler, request, callback);
    } else if (stmt instanceof SQLShowColumnsStatement) {

    } else if (stmt instanceof SQLShowTablesStatement) {
      invokeHandlerExecute(showTablesHandler, request, callback);
    } else if (stmt instanceof SQLUseStatement) {

    } else if (stmt instanceof SQLRollbackStatement) {

    } else if (stmt instanceof SQLCommitStatement) {

    } else if (stmt instanceof MySqlKillStatement) {
      invokeHandlerExecute(killStatementHandler, request, callback);
    } else if (stmt instanceof MySqlShowProcessListStatement) {

    } else if (stmt instanceof SQLDescribeStatement) {

    } else if (stmt instanceof SQLSetStatement) {
      invokeHandlerExecute(setStatementHandler, request, callback);
    } else if (stmt instanceof SQLShowMetricsStatement) {
      invokeHandlerExecute(showMetricsHandler, request, callback);
    } else {
      submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
    }
  }
}
