package com.gllue.command.handler.query.ddl.create;

import static com.gllue.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.cluster.ClusterState;
import com.gllue.command.handler.CommandHandlerException;
import com.gllue.command.handler.query.DefaultHandlerResult;
import com.gllue.command.handler.query.QueryHandlerRequest;
import com.gllue.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.common.Callback;
import com.gllue.common.Promise;
import com.gllue.common.exception.BadDatabaseException;
import com.gllue.common.exception.TableExistsException;
import com.gllue.config.Configurations;
import com.gllue.metadata.command.MetaDataCommand;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.repository.PersistRepository;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.core.service.TransportService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateTableHandler extends AbstractDDLHandler {
  private static final String NAME = "Create table handler";

  public CreateTableHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<DefaultHandlerResult> callback) {
    var stmt = (MySqlCreateTableStatement) request.getStatement();
    var attributes = request.getCommentsAttributes();
    var encryptProcessor = new EncryptColumnProcessor();
    var tablePartitionProcessor = new TablePartitionProcessor(configurations, attributes);
    if (!encryptProcessor.prepare(stmt) && !tablePartitionProcessor.prepare(stmt)) {
      // Do nothing for the create table query.
      submitQueryToBackendDatabase(request, request.getQuery(), callback);
      return;
    }

    var datasource = request.getDatasource();
    var database = request.getDatabase();
    var tableName = unquoteName(stmt.getTableName());
    var databaseMetaData = clusterState.getMetaData().getDatabase(datasource, database);
    if (databaseMetaData == null) {
      throw new BadDatabaseException(database);
    }
    if (databaseMetaData.hasTable(tableName)) {
      // Table already exists.
      if (!stmt.isIfNotExists()) {
        throw new TableExistsException(tableName);
      }
      callback.onSuccess(DefaultHandlerResult.getInstance());
      return;
    }

    var stmtList = tablePartitionProcessor.processStatement(List.of(stmt));

    MetaDataCommand<MultiDatabasesMetaData> createTableCommand =
        buildCreateTableCommand(
            datasource, database, tableName, stmtList, tablePartitionProcessor.isPrepared());

    stmtList = encryptProcessor.processStatement(stmtList);

    Promise.chain(createTablePromiseSupplier(request, stmtList))
        .then(
            (v) -> {
              createTableCommand.execute(newCommandExecutionContext());
              callback.onSuccess(DefaultHandlerResult.getInstance());
              return null;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return null;
            });
  }
}
