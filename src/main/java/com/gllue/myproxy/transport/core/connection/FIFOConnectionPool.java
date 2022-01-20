package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

/** A connection pool that follows the first in first out rule. */
public class FIFOConnectionPool extends AbstractConnectionPool {
  private static final int MAX_POLL_EFFORT = 10;

  private final ConnectionArguments arguments;
  private final ConnectionFactory connectionFactory;
  private final Queue<QueueEntry> queue = new ConcurrentLinkedQueue<>();

  public FIFOConnectionPool(
      final ConnectionArguments arguments,
      ConnectionFactory connectionFactory,
      final int maxPoolSize,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    this(
        arguments,
        connectionFactory,
        maxPoolSize,
        DEFAULT_IDLE_TIMEOUT_IN_MILLS,
        DEFAULT_KEEP_ALIVE_TIME_IN_MILLS,
        DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
        DEFAULT_MAX_LIFE_TIME_IN_MILLS,
        scheduler,
        executor);
  }

  public FIFOConnectionPool(
      final ConnectionArguments arguments,
      ConnectionFactory connectionFactory,
      final int maxPoolSize,
      final long idleTimeoutInMills,
      final long keepAliveTimeInMills,
      final long keepAliveQueryTimeoutInMills,
      final long maxLifeTimeInMills,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    super(
        maxPoolSize,
        idleTimeoutInMills,
        keepAliveTimeInMills,
        keepAliveQueryTimeoutInMills,
        maxLifeTimeInMills,
        scheduler,
        executor);
    this.arguments = arguments;
    this.connectionFactory = connectionFactory;
  }

  @Override
  public int getNumberOfCachedConnections() {
    return queue.size();
  }

  @Override
  public PooledConnection newPooledConnection(PoolEntry entry){
    return new PooledConnection(entry, this);
  }

  @Override
  ExtensibleFuture<Connection> newConnection(@Nullable String database) {
    return connectionFactory.newConnection(arguments, database);
  }

  @Override
  PoolEntry newEntry(Connection connection) {
    return new QueueEntry(connection);
  }

  @Override
  boolean offerEntry(PoolEntry entry) {
    var queueEntry = (QueueEntry) entry;
    if (queueEntry.setStateUnused()) {
      return queue.offer(queueEntry);
    }
    return false;
  }

  private PoolEntry pollEntry0() {
    QueueEntry entry;
    int retryCount = 0;
    do {
      entry = queue.poll();
      if (entry == null) {
        return null;
      }

      if (!entry.setStateUsing()) {
        if (!entry.isRemoved()) {
          queue.offer(entry);
        }
        entry = null;
      }
    } while (entry == null && retryCount++ < MAX_POLL_EFFORT);
    return entry;
  }

  private PoolEntry pollEntryWithDatabase(String database) {
    var iterator = queue.iterator();
    for (int i = 0; i < MAX_POLL_EFFORT && iterator.hasNext(); i++) {
      var entry = iterator.next();
      if (database.equals(entry.getConnection().currentDatabase())) {
        if (entry.setStateUsing()) {
          iterator.remove();
          return entry;
        }
      }
    }
    return pollEntry0();
  }

  @Override
  PoolEntry pollEntry(@Nullable String database) {
    if (database != null) {
      return pollEntryWithDatabase(database);
    }
    return pollEntry0();
  }

  @Override
  boolean reserveEntry(PoolEntry entry) {
    var queueEntry = (QueueEntry) entry;
    return queueEntry.setStateReserved();
  }

  @Override
  boolean unreserveEntry(PoolEntry entry) {
    var queueEntry = (QueueEntry) entry;
    return queueEntry.setStateUnreserved();
  }

  @Override
  boolean removeEntry(PoolEntry entry) {
    var queueEntry = (QueueEntry) entry;
    if (queueEntry.setStateRemoved()) {
      return queue.remove(queueEntry);
    }
    return false;
  }

  @Override
  Iterable<PoolEntry> entries() {
    return queue.stream().filter(QueueEntry::isUnused).collect(Collectors.toList());
  }

  enum EntryState {
    USING,
    UNUSED,
    RESERVED,
    REMOVED,
  }

  @RequiredArgsConstructor
  static class QueueEntry implements PoolEntry {
    private final Connection connection;
    private final AtomicReference<EntryState> state = new AtomicReference<>(EntryState.USING);

    boolean isUnused() {
      return state.get() == EntryState.UNUSED;
    }

    boolean isRemoved() {
      return state.get() == EntryState.REMOVED;
    }

    boolean setStateUsing() {
      return state.compareAndSet(EntryState.UNUSED, EntryState.USING);
    }

    boolean setStateUnused() {
      return state.compareAndSet(EntryState.USING, EntryState.UNUSED);
    }

    boolean setStateReserved() {
      return state.compareAndSet(EntryState.UNUSED, EntryState.RESERVED);
    }

    boolean setStateUnreserved() {
      return state.compareAndSet(EntryState.RESERVED, EntryState.UNUSED);
    }

    boolean setStateRemoved() {
      if (!state.compareAndSet(EntryState.UNUSED, EntryState.REMOVED)) {
        return state.compareAndSet(EntryState.RESERVED, EntryState.REMOVED);
      }
      return true;
    }

    @Override
    public Connection getConnection() {
      return connection;
    }
  }
}
