package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import lombok.Getter;

/**
 * Kill the server connection by <id>.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-process-kill.html#packet-COM_PROCESS_KILL">COM_PROCESS_KILL</a>
 */
public class ProcessKillCommandPacket extends AbstractCommandPacket {
  @Getter
  private final int connectionId;

  public ProcessKillCommandPacket(final int connectionId) {
    super(MySQLCommandPacketType.COM_PROCESS_KILL);

    this.connectionId = connectionId;
  }

  public ProcessKillCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_PROCESS_KILL);

    validateCommandType(payload);
    this.connectionId = payload.readInt4();
  }

  @Override
  public void write(MySQLPayload payload) {
    super.write(payload);
    payload.writeInt4(connectionId);
  }
}
