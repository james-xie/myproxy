package com.gllue.repository.zookeeper;

import com.gllue.repository.ClusterPersistRepository;
import com.gllue.repository.DataChangedEvent;
import com.gllue.repository.DataChangedEvent.Type;
import com.gllue.repository.DataChangedEventListener;
import com.gllue.repository.PersistRepositoryException;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.CreateMode;

@Slf4j
public class ZookeeperPersistRepository implements ClusterPersistRepository {

  private enum State {
    LATENT,
    STARTED,
    CLOSED
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

  private final ZookeeperConfigProperties properties;
  private CuratorFramework client;

  private final Map<String, CuratorCache> caches = new ConcurrentHashMap<>();

  private final Map<String, InterProcessLock> locks = new ConcurrentHashMap<>();

  public ZookeeperPersistRepository(final ZookeeperConfigProperties properties) {
    this.properties = properties;
  }

  @Override
  public void init() {
    Preconditions.checkState(
        state.compareAndSet(State.LATENT, State.STARTED), "Already initialized.");

    this.client = buildClient();
    startClient(client);
  }

  private CuratorFramework buildClient() {
    var builder = CuratorFrameworkFactory.builder();
    int retryBaseTimeMs = properties.getValue(ZookeeperConfigPropertyKey.RETRY_BASE_TIME_MS);
    int maxRetries = properties.getValue(ZookeeperConfigPropertyKey.MAX_RETRIES);
    int retryMaxTimeMs = properties.getValue(ZookeeperConfigPropertyKey.RETRY_MAX_TIME_MS);
    String address = properties.getValue(ZookeeperConfigPropertyKey.ADDRESS);
    int sessionTimeoutMs = properties.getValue(ZookeeperConfigPropertyKey.SESSION_TIMEOUT_MS);
    int operationTimeoutMs = properties.getValue(ZookeeperConfigPropertyKey.OPERATION_TIMEOUT_MS);
    return builder
        .connectString(address)
        .sessionTimeoutMs(sessionTimeoutMs)
        .connectionTimeoutMs(operationTimeoutMs)
        .retryPolicy(new ExponentialBackoffRetry(retryBaseTimeMs, maxRetries, retryMaxTimeMs))
        .build();
  }

  private void startClient(CuratorFramework client) {
    client.start();

    int maxWaitTime = properties.getValue(ZookeeperConfigPropertyKey.CONNECT_TIMEOUT_MS);
    try {
      client.blockUntilConnected(maxWaitTime, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      var msg = "Interrupted while waiting for connect to the zookeeper server.";
      throw new PersistRepositoryException(msg, e);
    }
  }

  private CuratorCache addCache(final String path) {
    var cache = CuratorCache.build(client, path);
    cache.start();

    var old = caches.putIfAbsent(path, cache);
    if (old != null) {
      throw new IllegalArgumentException(
          String.format("Cache for path [%s] already exists.", path));
    }
    return cache;
  }

  @Override
  public void watch(String key, DataChangedEventListener listener) {
    assert key.startsWith(PATH_SEPARATOR);
    if (caches.containsKey(key)) {
      return;
    }

    var cache = addCache(key);
    TreeCacheListener treeCacheListener =
        (client, event) -> {
          var eventType = getChangeEventType(event.getType());
          if (eventType != null) {
            listener.onChange(
                new DataChangedEvent(
                    event.getData().getPath(), event.getData().getData(), eventType));
          }
        };

    // The listener becomes active once the cache has been initialized.
    var cacheListener =
        CuratorCacheListener.builder()
            .forTreeCache(client, treeCacheListener)
            .afterInitialized()
            .build();
    cache.listenable().addListener(cacheListener);
  }

  private Type getChangeEventType(TreeCacheEvent.Type eventType) {
    switch (eventType) {
      case NODE_ADDED:
        return Type.CREATED;
      case NODE_UPDATED:
        return Type.UPDATED;
      case NODE_REMOVED:
        return Type.DELETED;
    }
    return null;
  }

  private InterProcessLock getLock(final String key) {
    var lock = locks.get(key);
    if (lock != null) {
      return lock;
    }

    lock = new InterProcessMutex(client, key);
    if (locks.putIfAbsent(key, lock) == null) {
      return lock;
    }

    return locks.get(key);
  }

  @Override
  public boolean tryLock(String key, long time, TimeUnit unit) {
    boolean locked = false;
    try {
      locked = getLock(key).acquire(time, unit);
    } catch (Exception e) {
      handleException(e);
    }
    return locked;
  }

  @Override
  public void releaseLock(String key) {
    try {
      getLock(key).release();
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public byte[] get(String key) {
    byte[] data = null;
    try {
      data = client.getData().forPath(key);
    } catch (Exception e) {
      handleException(e);
    }
    return data;
  }

  @Override
  public boolean exists(String key) {
    var exists = false;
    try {
      exists = client.checkExists().forPath(key) != null;
    } catch (Exception e) {
      handleException(e);
    }
    return exists;
  }

  @Override
  public List<String> getChildrenKeys(String key) {
    List<String> keys;
    try {
      keys = client.getChildren().forPath(key);
      keys.sort(Comparator.reverseOrder());
    } catch (final Exception ex) {
      handleException(ex);
      keys = Collections.emptyList();
    }
    return keys;
  }

  @Override
  public void save(String key, byte[] data) {
    try {
      client
          .create()
          .orSetData()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(key, data);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void delete(String key) {
    try {
      client.delete().quietly().deletingChildrenIfNeeded().forPath(key);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void close() {
    if (state.compareAndSet(State.STARTED, State.CLOSED)) {
      caches.values().forEach(CuratorCache::close);
      waitForCacheClose();
      CloseableUtils.closeQuietly(client);
    }
  }

  /* TODO wait 500ms, close cache before close client, or will throw exception
   * Because of asynchronous processing, may cause client to close
   * first and cache has not yet closed the end.
   * Wait for new version of Curator to fix this.
   * BUG address: https://issues.apache.org/jira/browse/CURATOR-157
   */
  private void waitForCacheClose() {
    try {
      Thread.sleep(500L);
    } catch (final InterruptedException ex) {
      handleException(ex);
    }
  }

  private void handleException(final Exception cause) {
    ThreadUtils.checkInterrupted(cause);
    throw new PersistRepositoryException(cause);
  }
}
