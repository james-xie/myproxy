package com.gllue.common.concurrent;

import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SettableFuture<T> extends Future<T> {
  /**
   * Set the value if the operation has done.
   *
   * @param value the operation result.
   * @return true if the value was successfully changed.
   */
  boolean set(@Nullable T value);

  /**
   * Set the exception if the operation has failed.
   *
   * @param throwable the cause of the failure.
   * @return true if the exception was successfully changed.
   */
  boolean setException(@Nonnull Throwable throwable);
}
