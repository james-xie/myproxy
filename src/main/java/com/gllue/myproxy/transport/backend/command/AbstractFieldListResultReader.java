package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import com.gllue.myproxy.transport.exception.CustomErrorCode;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinitionPacketWrapper;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;

public abstract class AbstractFieldListResultReader extends AbstractCommandResultReader {
  private boolean readCompleted = false;

  @Override
  public boolean doRead(MySQLPayload payload) {
    readColumnDef(payload);
    return readCompleted;
  }

  protected ColumnDefinitionPacketWrapper readColumnDef(final MySQLPayload payload) {
    var wrapper = ColumnDefinitionPacketWrapper.newInstance(payload, true);
    if (wrapper.isErrPacket()) {
      handleErrPacket((ErrPacket) wrapper.getPacket());
      readCompleted = true;
    } else if (wrapper.isEofPacket()) {
      handleEofPacket((EofPacket) wrapper.getPacket());
      readCompleted = true;
    } else {
      onColumnRead((ColumnDefinition41Packet) wrapper.getPacket());
    }
    return wrapper;
  }

  private void handleErrPacket(final ErrPacket packet) {
    onFailure(new BackendResultReadException(CustomErrorCode.newInstance(packet)));
  }

  private void handleEofPacket(final EofPacket packet) {
    onSuccess(new CommandResult(0, 0, packet.getStatusFlags(), packet.getWarnings(), null, null));
  }

  protected abstract void onColumnRead(final ColumnDefinition41Packet packet);

  @Override
  public boolean isReadCompleted() {
    return readCompleted;
  }
}
