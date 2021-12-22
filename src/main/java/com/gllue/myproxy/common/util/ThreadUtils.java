package com.gllue.myproxy.common.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ThreadUtils {
  public static boolean checkInterrupted(Throwable e)
  {
    if ( e instanceof InterruptedException )
    {
      Thread.currentThread().interrupt();
      return true;
    }
    return false;
  }
}
