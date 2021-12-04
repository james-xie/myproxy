package com.gllue.transport.backend.command;

import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.common.concurrent.ThreadPool;
import com.gllue.transport.core.connection.Connection;
import com.gllue.transport.protocol.payload.MySQLPayload;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCommandResultReader implements CommandResultReader {
  private final List<Callback<CommandResult>> callbacks;
  private CommandResult commandResult;
  private Throwable throwable;
  private boolean prepared = false;

  private WeakReference<Connection> connectionRef;

  public AbstractCommandResultReader() {
    this.callbacks = new ArrayList<>();
  }

  protected void prepareRead() {}

  abstract boolean doRead(MySQLPayload payload);

  @Override
  public boolean read(MySQLPayload payload) {
    if (!prepared) {
      prepareRead();
      prepared = true;
    }
    return doRead(payload);
  }

  @Override
  public void bindConnection(Connection connection) {
    assert this.connectionRef == null;
    this.connectionRef = new WeakReference<>(connection);
  }

  protected Connection getConnection() {
    return connectionRef.get();
  }

  @Override
  public CommandResultReader addCallback(Callback<CommandResult> callback) {
    this.callbacks.add(callback);
    return this;
  }

  @Override
  public Executor executor() {
    return ThreadPool.DIRECT_EXECUTOR_SERVICE;
  }

  @Override
  public void close() throws Exception {
    if (!getConnection().isAutoRead()) {
      getConnection().enableAutoRead();
    }
    connectionRef = null;
  }

  @Override
  public void fireReadCompletedEvent() {
    if (!isReadCompleted()) {
      return;
    }

    if (throwable != null) {
      for (var callback : callbacks) {
        callback.onFailure(throwable);
      }
    } else {
      assert commandResult != null;
      for (var callback : callbacks) {
        callback.onSuccess(commandResult);
      }
    }
  }

  /** Invoked with the result of the computation when it is successful. */
  protected void onSuccess(CommandResult result) {
    commandResult = result;
    throwable = null;
  }

  /** Invoked when a computation fails. */
  protected void onFailure(Throwable e) {
    commandResult = null;
    throwable = e;
  }
}
