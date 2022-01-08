package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.transport.core.connection.AdaptableTrafficThrottlePipe;
import com.gllue.myproxy.transport.core.connection.TrafficThrottlePipe;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;

public class DirectTransferCommandResultReader extends DefaultCommandResultReader {
  private final FrontendConnection frontendConnection;
  private TrafficThrottlePipe pipe;

  public DirectTransferCommandResultReader(final FrontendConnection frontendConnection) {
    this.frontendConnection = frontendConnection;
  }

  @Override
  protected void prepareRead() {
    super.prepareRead();
    pipe = new AdaptableTrafficThrottlePipe(getConnection(), frontendConnection);
    pipe.prepareToTransfer();
  }

  @Override
  protected GenericPacketWrapper readCommandResult(MySQLPayload payload) {
    var wrapper = super.readCommandResult(payload);
    pipe.transfer(wrapper.getPacket(), isReadCompleted());
    return wrapper;
  }

  @Override
  public void close() throws Exception {
    super.close();
    pipe.close();
  }
}
