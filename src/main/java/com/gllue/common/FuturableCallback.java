package com.gllue.common;

import com.gllue.common.concurrent.PlainFuture;
import com.gllue.common.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

public class FuturableCallback<V> implements Callback<V>, Future<V> {
  private SettableFuture<V> future;

  public FuturableCallback(SettableFuture<V> future) {
    this.future = future;
  }

  public FuturableCallback() {
    this(new PlainFuture<>());
  }

  @Override
  public void onSuccess(V result) {
    future.set(result);
  }

  @Override
  public void onFailure(Throwable e) {
    future.setException(e);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return future.get();
  }

  @Override
  public V get(long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, unit);
  }
}
