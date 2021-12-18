package com.gllue.myproxy.repository;

/**
 * Data changed listener.
 */
public interface DataChangedEventListener {

  /**
   * Fire when data changed.
   *
   * @param event data changed event
   */
  void onChange(DataChangedEvent event);
}

