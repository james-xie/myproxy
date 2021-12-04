package com.gllue.common.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PlainFuture<T> extends AbstractFuture<T> implements SettableFuture<T> {
  public static <T> PlainFuture<T> newFuture(final Runnable runnable) {
    return newFuture(runnable, ThreadPool.DIRECT_EXECUTOR_SERVICE);
  }

  public static <T> PlainFuture<T> newFuture(final Runnable runnable, final Executor executor) {
    var future = new PlainFuture<T>();
    future.addListener(runnable, executor);
    return future;
  }

  @Override
  public boolean set(@Nullable T value) {
    return super.set(value);
  }

  @Override
  public boolean setException(@Nonnull Throwable throwable) {
    return super.setException(throwable);
  }
}
