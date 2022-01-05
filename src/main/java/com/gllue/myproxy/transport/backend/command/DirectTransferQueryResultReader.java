package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.transport.core.connection.AdaptableTrafficThrottlePipe;
import com.gllue.myproxy.transport.core.connection.TrafficThrottlePipe;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnCountPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinitionPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.query.TextResultSetRowPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.gllue.myproxy.transport.protocol.payload.WrappedPayloadPacket;
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
    transfer(wrapper.getPacket());
    return wrapper;
  }

  @Override
  ColumnDefinitionPacketWrapper readColumnDef(MySQLPayload payload) {
    var packet = ColumnDefinitionPacketWrapper.tryMatch(payload);
    if (packet instanceof ErrPacket) {
      handleErrPacket((ErrPacket) packet);
    } else if (packet != null) {
      throw new IllegalStateException(String.format("Got an unexpected packet. [%s]", packet));
    }
    readColumnCount++;
    if (readColumnCount >= columnCount) {
      afterReadColumnDefinitions();
      state = State.READ_COLUMN_EOF;
    }

    if (packet == null) {
      transfer(payload);
    } else {
      transfer(packet);
    }
    return null;
  }

  @Override
  GenericPacketWrapper readColumnEof(MySQLPayload payload) {
    var wrapper = super.readColumnEof(payload);
    transfer(wrapper.getPacket());
    return wrapper;
  }

  @Override
  TextResultSetRowPacketWrapper readRow(MySQLPayload payload) {
    var packet = TextResultSetRowPacketWrapper.tryMatch(payload);
    if (packet instanceof ErrPacket) {
      handleErrPacket((ErrPacket) packet);
    } else if (packet instanceof EofPacket) {
      afterReadRows();
      handleEofPacket((EofPacket) packet);
    } else if (packet instanceof OKPacket) {
      afterReadRows();
      handleOkPacket((OKPacket) packet);
    }

    if (packet == null) {
      transfer(payload);
    } else {
      transfer(packet);
    }
    return null;
  }

  @Override
  protected void onColumnRead(ColumnDefinition41Packet packet) {
    throw new UnsupportedOperationException(
        "onColumnRead is unsupported for DirectTransferQueryResultReader.");
  }

  @Override
  protected void onRowRead(TextResultSetRowPacket packet) {
    throw new UnsupportedOperationException(
        "onRowRead is unsupported for DirectTransferQueryResultReader.");
  }

  private void transfer(final MySQLPacket packet) {
    pipe.transfer(packet, isReadCompleted());
  }

  private void transfer(final MySQLPayload payload) {
    payload.retain();
    if (!pipe.transfer(new WrappedPayloadPacket(payload), isReadCompleted())) {
      payload.release();
    }
  }

  @Override
  public QueryResultMetaData getQueryResultMetaData() {
    throw new UnsupportedOperationException(
        "getQueryResultMetaData is unsupported for DirectTransferQueryResultReader.");
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (pipe != null) {
      pipe.close();
    }
  }
}
