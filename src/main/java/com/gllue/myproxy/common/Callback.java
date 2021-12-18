package com.gllue.myproxy.common;

/** A callback for accepting the results of a computation asynchronously. */
public interface Callback<V> {
  /** Invoked with the result of the computation when it is successful. */
  void onSuccess(V result);

  /** Invoked when a computation fails. */
  void onFailure(Throwable e);

  static <T> Callback<T> wrap(Callback<? super T> callback) {
    return new Callback<>() {
      @Override
      public void onSuccess(T result) {
        callback.onSuccess(result);
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onFailure(e);
      }
    };
  }
}
