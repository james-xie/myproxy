package com.gllue.myproxy.command.handler.query.ddl.create;

import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.exception.BadDatabaseException;
import com.gllue.myproxy.common.exception.TableExistsException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.MetaDataCommand;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
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
    ensureDatabaseExists(request);

    var stmt = (MySqlCreateTableStatement) request.getStatement();
    var attributes = request.getCommentsAttributes();
    var encryptProcessor = new EncryptColumnProcessor();
    var tablePartitionProcessor = new TablePartitionProcessor(configurations, attributes);
    var shouldEncrypt = encryptProcessor.prepare(stmt);
    var shouldPartition = tablePartitionProcessor.prepare(stmt);
    if (!shouldEncrypt && !shouldPartition) {
      // Do nothing for the create table query.
      submitQueryToBackendDatabase(request, request.getQuery(), callback);
      return;
    }

    var datasource = request.getDatasource();
    var databaseName = request.getDatabase();
    var tableName = unquoteName(stmt.getTableName());
    var database = clusterState.getMetaData().getDatabase(datasource, databaseName);
    if (database == null) {
      throw new BadDatabaseException(databaseName);
    }

    if (database.hasTable(tableName)) {
      // Table already exists.
      if (!stmt.isIfNotExists()) {
        throw new TableExistsException(tableName);
      }
      callback.onSuccess(QueryHandlerResult.OK_RESULT);
      return;
    }

    var stmtList = tablePartitionProcessor.processStatement(List.of(stmt));

    MetaDataCommand<MultiDatabasesMetaData> createTableCommand =
        buildCreateTableCommand(
            datasource, databaseName, tableName, stmtList, tablePartitionProcessor.isPrepared());

    stmtList = encryptProcessor.processStatement(stmtList);

    Promise.chain(createTablePromiseSupplier(request, stmtList))
        .then(
            (v) -> {
              createTableCommand.execute(newCommandExecutionContext());
              callback.onSuccess(new WrappedHandlerResult(v));
              return null;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return null;
            });
  }
}
