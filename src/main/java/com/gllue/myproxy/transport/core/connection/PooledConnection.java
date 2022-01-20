package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.transport.core.connection.ConnectionPool.PoolEntry;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PooledConnection extends DelegateConnection {
  private final WeakReference<ConnectionPool> poolRef;
  private final PoolEntry poolEntry;
  private final AtomicBoolean isReleased = new AtomicBoolean(false);

  public PooledConnection(final PoolEntry entry, final ConnectionPool pool) {
    super(entry.getConnection());
    this.poolEntry = entry;
    this.poolRef = new WeakReference<>(pool);
  }

  public PoolEntry getPoolEntry() {
    return poolEntry;
  }

  @Override
  public void close() {
    close((c) -> {});
  }

  @Override
  public void close(Consumer<Connection> onClosed) {
    if (isReleased.compareAndSet(false, true)) {
      var pool = poolRef.get();
      if (pool != null) {
        pool.releaseConnection(connection);
      } else {
        log.error("Connection pool object has been garbage collected before pooled connection.");
      }
      onClosed.accept(this.connection);
    }
  }
}
