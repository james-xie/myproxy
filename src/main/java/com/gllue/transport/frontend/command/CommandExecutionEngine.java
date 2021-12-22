package com.gllue.transport.frontend.command;

import com.gllue.cluster.ClusterState;
import com.gllue.command.handler.HandlerExecutor;
import com.gllue.command.handler.HandlerResult;
import com.gllue.command.handler.query.ConcreteQueryHandler;
import com.gllue.command.handler.query.DefaultHandlerResult;
import com.gllue.command.handler.query.QueryHandlerRequest;
import com.gllue.command.handler.query.QueryHandlerRequestImpl;
import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.common.concurrent.AbstractRunnable;
import com.gllue.common.concurrent.PlainFuture;
import com.gllue.common.concurrent.ThreadPool;
import com.gllue.common.concurrent.ThreadPool.Name;
import com.gllue.config.Configurations;
import com.gllue.repository.PersistRepository;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.backend.command.DefaultCommandResultReader;
import com.gllue.transport.backend.command.DirectTransferCommandResultReader;
import com.gllue.transport.backend.command.DirectTransferFieldListResultReader;
import com.gllue.transport.backend.connection.BackendConnection;
import com.gllue.transport.core.service.TransportService;
import com.gllue.transport.exception.ExceptionResolver;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;
import com.gllue.transport.exception.UnsupportedCommandException;
import com.gllue.transport.frontend.connection.FrontendConnection;
import com.gllue.transport.protocol.packet.command.CommandPacket;
import com.gllue.transport.protocol.packet.command.CreateDBCommandPacket;
import com.gllue.transport.protocol.packet.command.DropDBCommandPacket;
import com.gllue.transport.protocol.packet.command.FieldListCommandPacket;
import com.gllue.transport.protocol.packet.command.InitDBCommandPacket;
import com.gllue.transport.protocol.packet.command.ProcessKillCommandPacket;
import com.gllue.transport.protocol.packet.command.QueryCommandPacket;
import com.gllue.transport.protocol.packet.command.SimpleCommandPacket;
import com.gllue.transport.protocol.packet.generic.ErrPacket;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import com.google.common.base.Strings;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandExecutionEngine {
  private final ThreadPool threadPool;
  private final TransportService transportService;
  private final HandlerExecutor handlerExecutor;

  private final ConcreteQueryHandler concreteQueryHandler;

  public CommandExecutionEngine(
      final ThreadPool threadPool,
      final TransportService transportService,
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final SQLParser sqlParser) {
    this.threadPool = threadPool;
    this.transportService = transportService;
    this.handlerExecutor = new HandlerExecutor(threadPool);
    this.concreteQueryHandler =
        new ConcreteQueryHandler(
            repository, configurations, clusterState, transportService, sqlParser);
  }

  @RequiredArgsConstructor
  private class CommandRunner extends AbstractRunnable {
    private final FrontendConnection frontendConnection;
    private final CommandPacket packet;

    @Override
    protected void doRun() throws Exception {
      var backendConnection = frontendConnection.getBackendConnection();
      if (backendConnection == null) {
        backendConnection = transportService.assignBackendConnection(frontendConnection).get();
      }
      internalExecute(frontendConnection, packet, backendConnection);
    }

    @Override
    public void onFailure(Exception e) {
      ErrPacket packet = null;
      if (e instanceof InterruptedException || e instanceof ExecutionException) {
        log.error("Failed to acquire backend connection.", e);
      } else if (e instanceof CancellationException) {
        log.error("Future of get backend connection was cancelled.");
      } else {
        packet = ExceptionResolver.resolve(e);
      }

      if (packet != null) {
        frontendConnection.writeAndFlush(packet, PlainFuture.newFuture(frontendConnection::close));
      } else {
        frontendConnection.close();
      }
    }
  }

  public void execute(final FrontendConnection frontendConnection, final CommandPacket packet) {
    var backendConnection = frontendConnection.getBackendConnection();
    if (backendConnection == null) {
      threadPool.executor(Name.GENERIC).submit(new CommandRunner(frontendConnection, packet));
    } else {
      internalExecute(frontendConnection, packet, backendConnection);
    }
  }

  private void internalExecute(
      final FrontendConnection frontendConnection,
      final CommandPacket packet,
      final BackendConnection backendConnection) {
    if (backendConnection.isClosed()) {
      frontendConnection.writeAndFlush(new ErrPacket(ServerErrorCode.ER_LOST_BACKEND_CONNECTION));
      frontendConnection.close();
      return;
    }

    var currentDatabase = frontendConnection.currentDatabase();
    if (currentDatabase == null || backendConnection.currentDatabase().equals(currentDatabase)) {
      dispatchCommand(frontendConnection, packet, backendConnection);
    } else {
      changeDatabase(frontendConnection, packet, backendConnection, currentDatabase);
    }
  }

  private void changeDatabase(
      final FrontendConnection frontendConnection,
      final CommandPacket packet,
      final BackendConnection backendConnection,
      final String database) {
    backendConnection.sendCommand(
        new InitDBCommandPacket(database),
        DefaultCommandResultReader.newInstance(
            new Callback<>() {
              @Override
              public void onSuccess(CommandResult result) {
                backendConnection.changeDatabase(database);
                dispatchCommand(frontendConnection, packet, backendConnection);
              }

              @Override
              public void onFailure(Throwable e) {
                var packet = ExceptionResolver.resolve(e);
                frontendConnection.writeAndFlush(
                    packet, PlainFuture.newFuture(frontendConnection::close));
              }
            }));
  }

  private void dispatchCommand(
      final FrontendConnection frontendConnection,
      final CommandPacket packet,
      final BackendConnection backendConnection) {
    var commandType = packet.getCommandType();
    if (log.isDebugEnabled()) {
      log.debug("Executing command type: " + commandType.name());
    }

    switch (commandType) {
      case COM_QUIT:
        quit(frontendConnection);
        break;
      case COM_INIT_DB:
        initDB(frontendConnection, (InitDBCommandPacket) packet, backendConnection);
        break;
      case COM_QUERY:
        query(frontendConnection, (QueryCommandPacket) packet, backendConnection);
        break;
      case COM_FIELD_LIST:
        fieldList(frontendConnection, (FieldListCommandPacket) packet, backendConnection);
        break;
      case COM_CREATE_DB:
        createDB(frontendConnection, (CreateDBCommandPacket) packet, backendConnection);
        break;
      case COM_DROP_DB:
        dropDB(frontendConnection, (DropDBCommandPacket) packet, backendConnection);
        break;
      case COM_STATISTICS:
        statistics(frontendConnection, (SimpleCommandPacket) packet, backendConnection);
        break;
      case COM_PROCESS_INFO:
        processInfo(frontendConnection, (SimpleCommandPacket) packet, backendConnection);
        break;
      case COM_PROCESS_KILL:
        kill(frontendConnection, (ProcessKillCommandPacket) packet, backendConnection);
        break;
      case COM_PING:
        ping(frontendConnection, (SimpleCommandPacket) packet, backendConnection);
        break;
      default:
        throw new UnsupportedCommandException(commandType.name());
    }
  }

  /** Close the connection. */
  private void quit(final FrontendConnection frontendConnection) {
    frontendConnection.close();
  }

  /** Change the default schema of the connection */
  private void initDB(
      final FrontendConnection frontendConnection,
      final InitDBCommandPacket packet,
      final BackendConnection backendConnection) {
    var schemaName = packet.getSchemaName();
    if (Strings.isNullOrEmpty(schemaName)) {
      writeErr(frontendConnection, MySQLServerErrorCode.ER_BAD_DB_ERROR);
      return;
    }

    if (schemaName.equals(frontendConnection.currentDatabase())) {
      writeOk(frontendConnection);
      return;
    }

    backendConnection.sendCommand(
        packet,
        new DirectTransferCommandResultReader(frontendConnection)
            .addCallback(
                new Callback<>() {
                  @Override
                  public void onSuccess(CommandResult result) {
                    frontendConnection.changeDatabase(schemaName);
                    backendConnection.changeDatabase(schemaName);
                  }

                  @Override
                  public void onFailure(Throwable e) {}
                }));
  }

  private QueryHandlerRequest buildHandlerRequest(
      final FrontendConnection frontendConnection, final QueryCommandPacket packet) {
    return new QueryHandlerRequestImpl(
        frontendConnection.connectionId(),
        frontendConnection.getDataSourceName(),
        frontendConnection.currentDatabase(),
        packet.getQuery());
  }

  private void writeHandlerResult(FrontendConnection connection, HandlerResult result) {
    if (result instanceof DefaultHandlerResult) {
      writeOk(connection);
    }
  }

  /** Execute text-based query immediately. */
  private void query(
      final FrontendConnection frontendConnection,
      final QueryCommandPacket packet,
      final BackendConnection backendConnection) {
    if (log.isDebugEnabled()) {
      log.debug("Executing query command: " + packet.getQuery());
    }

    handlerExecutor.execute(
        concreteQueryHandler,
        buildHandlerRequest(frontendConnection, packet),
        new Callback<HandlerResult>() {
          @Override
          public void onSuccess(HandlerResult result) {
            if (!result.isDirectTransferred()) {
              writeHandlerResult(frontendConnection, result);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            var packet = ExceptionResolver.resolve(e);
            frontendConnection.writeAndFlush(
                packet, PlainFuture.newFuture(frontendConnection::close));
          }
        });
  }

  /** Get the column definitions of a table. */
  private void fieldList(
      final FrontendConnection frontendConnection,
      final FieldListCommandPacket packet,
      final BackendConnection backendConnection) {
    if (log.isDebugEnabled()) {
      log.debug("Executing field list command: " + packet.getQuery());
    }

    backendConnection.sendCommand(
        packet, new DirectTransferFieldListResultReader(frontendConnection));
  }

  /** Create a schema. */
  private void createDB(
      final FrontendConnection frontendConnection,
      final CreateDBCommandPacket packet,
      final BackendConnection backendConnection) {
    backendConnection.sendCommand(
        packet, new DirectTransferCommandResultReader(frontendConnection));
  }

  /** Drop a schema. */
  private void dropDB(
      final FrontendConnection frontendConnection,
      final DropDBCommandPacket packet,
      final BackendConnection backendConnection) {
    backendConnection.sendCommand(
        packet, new DirectTransferCommandResultReader(frontendConnection));
  }

  /** Get a human readable string of internal statistics. */
  private void statistics(
      final FrontendConnection frontendConnection,
      final SimpleCommandPacket packet,
      final BackendConnection backendConnection) {
    backendConnection.sendCommand(
        packet, new DirectTransferCommandResultReader(frontendConnection));
  }

  /** Get a list of active threads. */
  private void processInfo(
      final FrontendConnection frontendConnection,
      final SimpleCommandPacket packet,
      final BackendConnection backendConnection) {
    backendConnection.sendCommand(
        packet, new DirectTransferCommandResultReader(frontendConnection));
  }

  /** Terminate a connection. */
  private void kill(
      final FrontendConnection frontendConnection,
      final ProcessKillCommandPacket packet,
      final BackendConnection backendConnection) {
    try {
      transportService.closeFrontendConnection(packet.getConnectionId());
      writeOk(frontendConnection);
    } catch (IllegalArgumentException e) {
      writeErr(
          frontendConnection, MySQLServerErrorCode.ER_NO_SUCH_THREAD, packet.getConnectionId());
    }
  }

  /** Check if the server is alive */
  private void ping(
      final FrontendConnection frontendConnection,
      final SimpleCommandPacket packet,
      final BackendConnection backendConnection) {
    backendConnection.sendCommand(
        packet, new DirectTransferCommandResultReader(frontendConnection));
  }

  private void writeOk(final FrontendConnection frontendConnection) {
    frontendConnection.writeAndFlush(new OKPacket());
  }

  private void writeErr(
      final FrontendConnection frontendConnection,
      final SQLErrorCode errorCode,
      final Object... errorMessageArguments) {
    frontendConnection.writeAndFlush(new ErrPacket(errorCode, errorMessageArguments));
  }

  private void writeErrAndClose(
      final FrontendConnection frontendConnection, final SQLErrorCode errorCode) {
    writeErr(frontendConnection, errorCode);
    frontendConnection.close();
  }
}
