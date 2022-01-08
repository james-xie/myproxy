package com.gllue.myproxy.common;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A Promise is a proxy for a value not necessarily known when the promise is created. It allows you
 * to associate handlers with an asynchronous action's eventual success value or failure reason.
 * This lets asynchronous methods return values like synchronous methods: instead of immediately
 * returning the final value, the asynchronous method returns a promise to supply the value at some
 * point in the future.
 *
 * <pre>
 * @see <a href="https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise"></a>
 * </pre>
 */
@Slf4j
public class Promise<R> {
  enum State {
    RUNNING,
    DONE,
    FAILED
  }

  private volatile R result;
  private volatile Throwable exception;
  private volatile State state = State.RUNNING;
  private volatile Function<R, ?> onSuccess;
  private volatile Function<Throwable, ?> onFailure;
  private volatile Object onFinished;
  private volatile Callback nextPromiseCallback;

  private static final VarHandle STATE;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup();
      STATE = l.findVarHandle(Promise.class, "state", State.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public Promise(Consumer<Callback<R>> resolver) {
    runResolver(resolver);
  }

  private void runResolver(Consumer<Callback<R>> resolver) {
    var promise = this;
    try {
      resolver.accept(
          new Callback<>() {
            @Override
            public void onSuccess(R result) {
              promise.handleResult(result);
            }

            @Override
            public void onFailure(Throwable e) {
              promise.handleException(e);
            }
          });
    } catch (Throwable e) {
      handleException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Object invokeOnFinishedFunction(Object onFinished, R result, Throwable exception) {
    if (onFinished instanceof Supplier) {
      return ((Supplier<?>) onFinished).get();
    } else if (onFinished instanceof BiFunction) {
      return ((BiFunction<R, Throwable, ?>) onFinished).apply(result, exception);
    } else {
      throw new IllegalStateException("Illegal <onFinished> type.");
    }
  }

  private void handleResult(R result) {
    if (STATE.compareAndSet(this, State.RUNNING, State.DONE)) {
      this.result = result;
      if (nextPromiseCallback != null) {
        invokeOnSuccess();
      }
    } else {
      log.warn("Promise state has already changed, ignore result [{}].", result);
    }
  }

  private void handleException(Throwable e) {
    if (STATE.compareAndSet(this, State.RUNNING, State.FAILED)) {
      this.exception = e;
      if (nextPromiseCallback != null) {
        invokeOnFailure();
      } else {
        log.error("Found an unhandled exception in the promise.", e);
      }
    } else {
      if (log.isWarnEnabled()) {
        log.warn("Promise state has already changed, ignore exception.", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void invokeOnSuccess() {
    assert nextPromiseCallback != null;

    try {
      Object res;
      if (onSuccess != null) {
        res = onSuccess.apply(result);
      } else if (onFinished != null) {
        res = invokeOnFinishedFunction(onFinished, result, exception);
      } else {
        res = result;
      }

      if (res instanceof Promise) {
        var promise = (Promise<?>) res;
        promise.then(
            (v) -> {
              nextPromiseCallback.onSuccess(v);
              return null;
            },
            (e) -> {
              nextPromiseCallback.onFailure(e);
              return null;
            });
      } else {
        nextPromiseCallback.onSuccess(res);
      }
    } catch (Throwable e) {
      nextPromiseCallback.onFailure(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void invokeOnFailure() {
    assert nextPromiseCallback != null;

    try {
      Object res = null;
      if (onFailure != null) {
        res = onFailure.apply(exception);
      } else if (onFinished != null) {
        res = invokeOnFinishedFunction(onFinished, result, exception);
      }

      if (res instanceof Promise) {
        var promise = (Promise<?>) res;
        promise.then(
            (v) -> {
              if (onFailure != null) {
                nextPromiseCallback.onSuccess(v);
              } else {
                nextPromiseCallback.onFailure(exception);
              }
              return null;
            },
            (e) -> {
              nextPromiseCallback.onFailure(e);
              return null;
            });
      } else {
        if (onFailure != null) {
          nextPromiseCallback.onSuccess(res);
        } else {
          nextPromiseCallback.onFailure(exception);
        }
      }

    } catch (Throwable e) {
      nextPromiseCallback.onFailure(e);
    }
  }

  private void ensureNoCallback() {
    if (this.nextPromiseCallback != null) {
      throw new IllegalStateException("This method cannot be called more than once.");
    }
  }

  private void tryInvoke() {
    if (this.state == State.DONE) {
      invokeOnSuccess();
    } else if (this.state == State.FAILED) {
      invokeOnFailure();
    }
  }

  private <T> Promise<T> then(
      Function<R, T> onSuccess, Function<Throwable, T> onFailure, Supplier<T> onFinished) {
    return new Promise<>(
        callback -> {
          ensureNoCallback();

          this.onSuccess = onSuccess;
          this.onFailure = onFailure;
          this.onFinished = onFinished;
          this.nextPromiseCallback = callback;

          tryInvoke();
        });
  }

  private <T> Promise<T> thenAsync(
      Function<R, Promise<T>> onSuccess,
      Function<Throwable, Promise<T>> onFailure,
      Object onFinished) {
    return new Promise<>(
        callback -> {
          ensureNoCallback();

          this.onSuccess = onSuccess;
          this.onFailure = onFailure;
          this.onFinished = onFinished;
          this.nextPromiseCallback = callback;

          tryInvoke();
        });
  }

  public <T> Promise<T> then(Function<R, T> onSuccess) {
    return then(onSuccess, null, null);
  }

  public <T> Promise<T> then(Function<R, T> onSuccess, Function<Throwable, T> onFailure) {
    return then(onSuccess, onFailure, null);
  }

  public <T> Promise<T> doCatch(Function<Throwable, T> onFailure) {
    return then(null, onFailure, null);
  }

  public <T> Promise<T> doFinally(Supplier<T> onFinished) {
    return then(null, null, onFinished);
  }

  public <T> Promise<T> doFinally(BiFunction<T, Throwable, T> onFinished) {
    return thenAsync(null, null, onFinished);
  }

  public <T> Promise<T> thenAsync(Function<R, Promise<T>> onSuccess) {
    return thenAsync(onSuccess, null, null);
  }

  public <T> Promise<T> thenAsync(
      Function<R, Promise<T>> onSuccess, Function<Throwable, Promise<T>> onFailure) {
    return thenAsync(onSuccess, onFailure, null);
  }

  public <T> Promise<T> doCatchAsync(Function<Throwable, Promise<T>> onFailure) {
    return thenAsync(null, onFailure, null);
  }

  public <T> Promise<T> doFinallyAsync(Supplier<Promise<T>> onFinished) {
    return thenAsync(null, null, onFinished);
  }

  public <T> Promise<T> doFinallyAsync(BiFunction<T, Throwable, Promise<T>> onFinished) {
    return thenAsync(null, null, onFinished);
  }

  public interface Promises<T> {
    List<Promise<T>> list();

    T[] newResult(int size);
  }

  public static <T> Promise<T> emptyPromise() {
    return new Promise<>((cb) -> cb.onSuccess(null));
  }

  public static <T> Promise<T> emptyPromise(T value) {
    return new Promise<>((cb) -> cb.onSuccess(value));
  }

  /**
   * The Promise.parallelAll() method takes an iterable of promises as an input, and returns a single
   * Promise that resolves to an array of the results of the input promises. This returned promise
   * will resolve when all of the input's promises have resolved, or if the input iterable contains
   * no promises. It rejects immediately upon any of the input promises rejecting or non-promises
   * throwing an error, and will reject with this first rejection message / error.
   *
   * <pre>
   * @see <a href="https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/all"></a>
   * </pre>
   */
  public static <T> Promise<T[]> parallelAll(Promises<T> promises) {
    return new Promise<>(
        (callback) -> {
          AtomicBoolean callbackLatch = new AtomicBoolean(false);
          Function<Throwable, T> onFailure =
              (e) -> {
                if (callbackLatch.compareAndSet(false, true)) {
                  callback.onFailure(e);
                }
                return null;
              };

          int i = 0;
          var list = promises.list();
          var allResult = promises.newResult(list.size());
          AtomicInteger counter = new AtomicInteger(allResult.length);

          for (var promise : list) {
            var index = i++;
            Function<T, T> onSuccess =
                (v) -> {
                  allResult[index] = v;
                  if (counter.decrementAndGet() == 0) {
                    if (callbackLatch.compareAndSet(false, true)) {
                      callback.onSuccess(allResult);
                    }
                  }
                  return v;
                };
            promise.then(onSuccess, onFailure);
          }
        });
  }

  private static <T> Promise<T> chain0(Function<T, Promise<T>> supplier, T value) {
    var promise = supplier.apply(value);
    if (promise == null) {
      return emptyPromise(value);
    }

    return promise.thenAsync((v) -> chain0(supplier, v));
  }

  public static <T> Promise<T> chain(Function<T, Promise<T>> supplier) {
    return chain0(supplier, null);
  }

  private static <T> Promise<List<T>> all0(Supplier<Promise<T>> supplier, List<T> result) {
    var promise = supplier.get();
    if (promise == null) {
      return emptyPromise(result);
    }

    return promise.thenAsync((v) -> {
      result.add(v);
      return all0(supplier, result);
    });
  }

  public static <T> Promise<List<T>> all(Supplier<Promise<T>> supplier) {
    var result = Collections.synchronizedList(new ArrayList<T>());;
    return all0(supplier, result);
  }
}
