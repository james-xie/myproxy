package com.gllue.transport.backend.command;

import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.transport.backend.BackendResultReadException;
import com.gllue.transport.exception.CustomErrorCode;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.protocol.packet.generic.ErrPacket;
import com.gllue.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;

public class DefaultCommandResultReader extends AbstractCommandResultReader {
  private boolean readCompleted = false;

  @Override
  public boolean doRead(MySQLPayload payload) {
    readCommandResult(payload);
    return true;
  }

  protected GenericPacketWrapper readCommandResult(final MySQLPayload payload) {
    var wrapper = GenericPacketWrapper.newInstance(payload);
    if (wrapper.isOkPacket()) {
      var packet = (OKPacket) wrapper.getPacket();
      onSuccess(CommandResult.newInstance(packet));
    } else if (wrapper.isErrPacket()) {
      var exception =
          new BackendResultReadException(
              CustomErrorCode.newInstance((ErrPacket) wrapper.getPacket()));
      onFailure(exception);
    } else {
      onFailure(new BackendResultReadException(MySQLServerErrorCode.ER_NET_READ_ERROR));
    }
    readCompleted = true;

    return wrapper;
  }

  @Override
  public boolean isReadCompleted() {
    return readCompleted;
  }

  public static DefaultCommandResultReader newInstance(Callback<CommandResult> callback) {
    var reader = new DefaultCommandResultReader();
    reader.addCallback(callback);
    return reader;
  }
}
