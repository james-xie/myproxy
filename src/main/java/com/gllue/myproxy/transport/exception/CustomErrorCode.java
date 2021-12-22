package com.gllue.myproxy.transport.exception;

import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CustomErrorCode implements SQLErrorCode {
  private final int errorCode;

  private final String sqlState;

  private final String errorMessage;

  public static CustomErrorCode newInstance(final ErrPacket packet) {
    return new CustomErrorCode(
        packet.getErrorCode(), packet.getSqlState(), packet.getErrorMessage());
  }
}
