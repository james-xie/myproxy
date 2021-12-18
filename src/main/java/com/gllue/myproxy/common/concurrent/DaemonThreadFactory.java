package com.gllue.myproxy.common.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

public class DaemonThreadFactory implements ThreadFactory {

  final ThreadGroup group;
  final String namePrefix;
  final AtomicInteger threadNumber = new AtomicInteger(1);

  public DaemonThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
    this.group = Thread.currentThread().getThreadGroup();
  }

  @Override
  public Thread newThread(@Nonnull Runnable r) {
    Thread t = new Thread(group, r, threadName(), 0);
    t.setDaemon(true);
    return t;
  }

  private String threadName() {
    return String.format("myproxy[%s][tid:%s]", namePrefix, threadNumber.getAndIncrement());
  }
}
