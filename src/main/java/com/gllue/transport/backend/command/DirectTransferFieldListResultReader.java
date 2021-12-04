package com.gllue.transport.backend.command;

import com.gllue.transport.core.connection.AdaptableTrafficThrottlePipe;
import com.gllue.transport.core.connection.TrafficThrottlePipe;
import com.gllue.transport.frontend.connection.FrontendConnection;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.transport.protocol.packet.query.ColumnDefinitionPacketWrapper;
import com.gllue.transport.protocol.payload.MySQLPayload;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectTransferFieldListResultReader extends AbstractFieldListResultReader {
  private final FrontendConnection frontendConnection;
  private TrafficThrottlePipe pipe;

  public DirectTransferFieldListResultReader(final FrontendConnection frontendConnection) {
    this.frontendConnection = frontendConnection;
  }

  @Override
  protected void prepareRead() {
    super.prepareRead();

    pipe = new AdaptableTrafficThrottlePipe(getConnection(), frontendConnection);
    pipe.prepareToTransfer();
  }

  @Override
  protected ColumnDefinitionPacketWrapper readColumnDef(MySQLPayload payload) {
    var wrapper = super.readColumnDef(payload);
    writePacket(wrapper.getPacket());
    return wrapper;
  }

  @Override
  protected void onColumnRead(ColumnDefinition41Packet packet) {}

  private void writePacket(final MySQLPacket packet) {
    pipe.transfer(packet, isReadCompleted());
  }

  @Override
  public void close() throws Exception {
    super.close();
    pipe.close();
  }
}
