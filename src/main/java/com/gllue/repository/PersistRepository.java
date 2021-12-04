package com.gllue.repository;

import com.gllue.constant.ServerConstants;
import java.util.List;

/** todo: implement nonblocking operations. */
public interface PersistRepository {

  /** Path separator. */
  String PATH_SEPARATOR = ServerConstants.PATH_SEPARATOR;

  /** Initialize repository. */
  void init();

  /**
   * Get data from registry center.
   *
   * @param key key of data
   * @return data
   */
  byte[] get(String key);

  /**
   * Whether the key is in the repository.
   *
   * @param key key of data
   * @return true if the key is already exists.
   */
  boolean exists(String key);

  /**
   * Get names of sub-node.
   *
   * @param key key of data
   * @return sub-node names
   */
  List<String> getChildrenKeys(String key);

  /**
   * Save the key value pair to the repository.
   *
   * @param key key of data
   * @param data data should be persisted.
   */
  void save(String key, byte[] data);

  /**
   * Delete by key.
   *
   * @param key key of data
   */
  void delete(String key);

  /** Close repository. */
  void close();
}
