package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.constant.TimeConstants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultIdleConnectionDetector implements IdleConnectionDetector {
  private static final int THRESHOLD_OF_FORCE_REMOVE_PENDING_CONNECTION = 5000;

  private static final Comparator<Connection> CONNECTION_ACCESS_TIME_COMPARATOR =
      Comparator.comparingLong(Connection::lastAccessTime);

  private final int maxIdleTimeInMills;
  private final PriorityQueue<Connection> queue;

  private final Set<Connection> pendingRegisterConnections;
  private final Set<Connection> pendingRemoveConnections;

  public DefaultIdleConnectionDetector(final int maxIdleTimeInSeconds) {
    this(maxIdleTimeInSeconds, true, true);
  }

  public DefaultIdleConnectionDetector(
      final int maxIdleTimeInSeconds, final boolean lazyRegister, final boolean lazyRemove) {
    this.maxIdleTimeInMills = maxIdleTimeInSeconds * TimeConstants.MILLS_PER_SECOND;
    this.queue = new PriorityQueue<>(CONNECTION_ACCESS_TIME_COMPARATOR);
    if (lazyRegister) {
      this.pendingRegisterConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    } else {
      this.pendingRegisterConnections = null;
    }
    if (lazyRemove) {
      this.pendingRemoveConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    } else {
      this.pendingRemoveConnections = null;
    }
  }

  @Override
  public void register(Connection connection) {
    if (pendingRegisterConnections != null) {
      pendingRegisterConnections.add(connection);
      return;
    }

    synchronized (queue) {
      queue.add(connection);
    }
  }

  @Override
  public void remove(Connection connection) {
    if (pendingRegisterConnections != null && pendingRegisterConnections.contains(connection)) {
      pendingRegisterConnections.remove(connection);
      return;
    }

    if (pendingRemoveConnections != null) {
      pendingRemoveConnections.add(connection);
      return;
    }

    synchronized (queue) {
      queue.remove(connection);
    }
  }

  @Override
  public Collection<Connection> detectIdleConnections(long currentTimeInMills) {
    Collection<Connection> idleConnections;
    synchronized (queue) {
      if (pendingRegisterConnections != null) {
        transferPendingRegisterConnections();
      }

      idleConnections = fetchIdleConnections(currentTimeInMills);

      if (pendingRemoveConnections != null) {
        forceRemovePendingConnections();
      }
    }
    return idleConnections;
  }

  private void transferPendingRegisterConnections() {
    var iterator = pendingRegisterConnections.iterator();
    while (iterator.hasNext()) {
      var connection = iterator.next();
      iterator.remove();

      if (pendingRemoveConnections == null) {
        queue.add(connection);
      } else {
        if (!pendingRemoveConnections.contains(connection)) {
          queue.add(connection);
        } else {
          pendingRemoveConnections.remove(connection);
        }
      }
    }
  }

  private void forceRemovePendingConnections() {
    if (pendingRemoveConnections.size() < THRESHOLD_OF_FORCE_REMOVE_PENDING_CONNECTION) {
      return;
    }

    queue.removeAll(pendingRemoveConnections);
  }

  private boolean isConnectionIdle(Connection connection, long currentTime) {
    return currentTime - connection.lastAccessTime() >= maxIdleTimeInMills;
  }

  private Collection<Connection> fetchIdleConnections(long currentTimeInMills) {
    var idleConnections = new ArrayList<Connection>();
    var connection = queue.peek();
    while (connection != null) {
      if (pendingRemoveConnections != null && pendingRemoveConnections.contains(connection)) {
        pendingRemoveConnections.remove(connection);
      } else {
        if (!isConnectionIdle(connection, currentTimeInMills)) {
          break;
        }
        idleConnections.add(connection);
      }

      queue.poll();
      connection = queue.peek();
    }
    return idleConnections;
  }
}
