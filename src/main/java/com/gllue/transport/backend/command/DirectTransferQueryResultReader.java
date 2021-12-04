package com.gllue.transport.backend.command;

import com.gllue.command.result.query.QueryResultMetaData;
import com.gllue.transport.core.connection.AdaptableTrafficThrottlePipe;
import com.gllue.transport.core.connection.TrafficThrottlePipe;
import com.gllue.transport.frontend.connection.FrontendConnection;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.transport.protocol.packet.query.ColumnCountPacketWrapper;
import com.gllue.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.transport.protocol.packet.query.ColumnDefinitionPacketWrapper;
import com.gllue.transport.protocol.packet.query.TextResultSetRowPacketWrapper;
import com.gllue.transport.protocol.packet.query.text.TextResultSetRowPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import lombok.extern.slf4j.Slf4j;

// todo: check CLIENT_DEPRECATE_EOF flag for frontend connection
@Slf4j
public class DirectTransferQueryResultReader extends AbstractQueryResultReader {
  private final FrontendConnection frontendConnection;
  private TrafficThrottlePipe pipe;

  public DirectTransferQueryResultReader(final FrontendConnection frontendConnection) {
    this.frontendConnection = frontendConnection;
  }

  @Override
  protected void prepareRead() {
    super.prepareRead();
    pipe = new AdaptableTrafficThrottlePipe(getConnection(), frontendConnection);
    pipe.prepareToTransfer();
  }

  @Override
  ColumnCountPacketWrapper readFirstPacket(MySQLPayload payload) {
    var wrapper = super.readFirstPacket(payload);
    writePacket(wrapper.getPacket());
    return wrapper;
  }

  @Override
  ColumnDefinitionPacketWrapper readColumnDef(MySQLPayload payload) {
    var wrapper = super.readColumnDef(payload);
    writePacket(wrapper.getPacket());
    return wrapper;
  }

  @Override
  GenericPacketWrapper readColumnEof(MySQLPayload payload) {
    var wrapper = super.readColumnEof(payload);
    writePacket(wrapper.getPacket());
    return wrapper;
  }

  @Override
  TextResultSetRowPacketWrapper readRow(MySQLPayload payload) {
    var wrapper = super.readRow(payload);
    writePacket(wrapper.getPacket());
    return wrapper;
  }

  @Override
  protected void onColumnRead(ColumnDefinition41Packet packet) {}

  @Override
  protected void onRowRead(TextResultSetRowPacket packet) {}

  private void writePacket(final MySQLPacket packet) {
    pipe.transfer(packet, isReadCompleted());
  }

  @Override
  public QueryResultMetaData getQueryResultMetaData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {
    super.close();
    pipe.close();
  }
}
