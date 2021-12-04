package com.gllue.repository;

import java.util.concurrent.TimeUnit;

public interface ClusterPersistRepository extends PersistRepository {
  /**
   * Watch the key and listen for the data change event under the key.
   *
   * @param key key of data
   * @param listener data changed event listener
   */
  void watch(String key, DataChangedEventListener listener);

  /**
   * Try to acquire lock under the lock key.
   *
   * @param key lock key
   * @param time time to wait
   * @param unit time unit
   * @return true if get the lock, false if not
   */
  boolean tryLock(String key, long time, TimeUnit unit);

  /**
   * Release lock under the key.
   *
   * @param key lock key
   */
  void releaseLock(String key);
}
