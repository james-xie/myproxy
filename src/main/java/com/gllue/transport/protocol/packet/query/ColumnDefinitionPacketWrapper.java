package com.gllue.transport.protocol.packet.query;

import com.gllue.transport.protocol.packet.AbstractPacketWrapper;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.generic.EofPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;

public class ColumnDefinitionPacketWrapper extends AbstractPacketWrapper {
  public ColumnDefinitionPacketWrapper(final MySQLPacket packet) {
    super(packet);
  }

  public static MySQLPacket tryMatch(final MySQLPayload payload, final boolean isCommandFieldList) {
    var packet = AbstractPacketWrapper.tryMatch(payload);
    if (packet != null) {
      return packet;
    } else if (EofPacket.match(payload)) {
      return new EofPacket(payload);
    }
    return new ColumnDefinition41Packet(payload, isCommandFieldList);
  }

  public static ColumnDefinitionPacketWrapper newInstance(
      final MySQLPayload payload, final boolean isCommandFieldList) {
    var packet = tryMatch(payload, isCommandFieldList);
    return new ColumnDefinitionPacketWrapper(packet);
  }

  public boolean isEofPacket() {
    return packet != null && packet instanceof EofPacket;
  }
}
