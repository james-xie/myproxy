package com.gllue.common.concurrent;

import com.google.common.util.concurrent.ListenableFuture;

public interface ExtensibleFuture<T> extends ListenableFuture<T> {
  /**
   * Return the result without blocking. If the future is not done yet this will return {@code null}.
   *
   * As it is possible that a {@code null} value is used to mark the future as successful you also need to check
   * if the future is really done with {@link #isDone()} and not rely on the returned {@code null} value.
   */
  T getValue();

  /**
   * Returns the exception if the operation has failed.
   *
   * @return the cause of the failure. {@code null} if succeeded or this future is not completed
   *     yet.
   */
  Throwable getException();

  /**
   * Returns {@code true} if and only if the operation was completed
   * successfully.
   */
  boolean isSuccess();
}
