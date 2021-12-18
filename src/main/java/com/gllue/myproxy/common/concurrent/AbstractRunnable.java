package com.gllue.myproxy.common.concurrent;

/** An extension to runnable. */
public abstract class AbstractRunnable implements Runnable {

  @Override
  public final void run() {
    try {
      doRun();
    } catch (Exception t) {
      onFailure(t);
    } finally {
      onAfter();
    }
  }

  /**
   * This method has the same semantics as {@link Runnable#run()}
   *
   * @throws InterruptedException if the run method throws an InterruptedException
   */
  protected abstract void doRun() throws Exception;

  /** This method is called in a finally block after successful execution or on a rejection. */
  public void onAfter() {}

  /** This method is invoked for all exception thrown by {@link #doRun()} */
  public abstract void onFailure(Exception e);

  /**
   * This should be executed if the thread-pool executing this action rejected the execution. The
   * default implementation forwards to {@link #onFailure(Exception)}
   */
  public void onRejection(Exception e) {
    onFailure(e);
  }

}
