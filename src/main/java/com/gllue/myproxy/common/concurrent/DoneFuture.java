package com.gllue.myproxy.common.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ExecutionList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

public final class DoneFuture<T> implements ExtensibleFuture<T> {

  // The execution list to hold our executors.
  private final ExecutionList executionList = new ExecutionList();

  private final T value;
  private final Throwable exception;

  public DoneFuture(T value) {
    Preconditions.checkNotNull(value);
    this.value = value;
    this.exception = null;
    // Make the new listeners execute immediately after the executionList is executed.
    this.executionList.execute();
  }

  public DoneFuture(Throwable exception) {
    Preconditions.checkNotNull(exception);
    this.value = null;
    this.exception = exception;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    if (value == null) {
      throw new ExecutionException(exception);
    }
    return value;
  }

  @Override
  public T get(long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return get();
  }

  @Override
  public void addListener(@Nonnull Runnable listener, @Nonnull Executor exec) {
    executionList.add(listener, exec);
  }

  @Override
  public boolean isSuccess() {
    return exception == null;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public Throwable getException() {
    return exception;
  }
}
