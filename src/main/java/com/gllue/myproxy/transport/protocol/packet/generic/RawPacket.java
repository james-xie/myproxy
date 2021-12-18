package com.gllue.myproxy.transport.protocol.packet.generic;

import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class RawPacket implements MySQLPacket {

  private final byte[] rawData;

  public RawPacket(final MySQLPayload payload) {
    rawData = payload.readStringEOFReturnBytes();
  }

  @Override
  public void write(MySQLPayload payload) {
    payload.writeBytes(rawData);
  }
}
