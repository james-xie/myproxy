package com.gllue.myproxy.transport.exception;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExceptionResolver {
  public static ErrPacket resolve(final Throwable e) {
    ErrPacket packet = null;
    if (e instanceof BaseServerException) {
      packet = resolveBaseServerException((BaseServerException) e);
    } else if (e instanceof IllegalArgumentException) {
      packet = resolveIllegalArgumentException((IllegalArgumentException) e);
    }

    if (packet != null) {
      if (log.isDebugEnabled()) {
        log.debug("Resolved exception.", e);
      }
      return packet;
    }

    log.error("Unable to resolve the exception.", e);

    return new ErrPacket(ServerErrorCode.ER_SERVER_ERROR, e.getMessage());
  }

  public static ErrPacket resolveBaseServerException(final BaseServerException e) {
    var rootCause = e.getRootCause();
    if (rootCause instanceof BaseServerException) {
      var cause = (BaseServerException) rootCause;
      return new ErrPacket(cause.getErrorCode(), cause.getErrorMessageArgs());
    }
    return resolve(rootCause);
  }

  public static ErrPacket resolveIllegalArgumentException(final IllegalArgumentException e) {
    return new ErrPacket(ServerErrorCode.ER_ILLEGAL_ARGUMENT, e.getMessage());
  }
}
