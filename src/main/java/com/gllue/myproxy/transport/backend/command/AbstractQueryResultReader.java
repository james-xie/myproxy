package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import com.gllue.myproxy.transport.exception.CustomErrorCode;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnCountPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnCountPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinitionPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.query.TextResultSetRowPacketWrapper;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractQueryResultReader extends AbstractCommandResultReader
    implements QueryResultReader {
  protected State state = State.READ_FIRST_PACKET;

  protected int columnCount = 0;
  protected int readColumnCount = 0;

  @Override
  public final boolean doRead(MySQLPayload payload) {
    switch (state) {
      case READ_FIRST_PACKET:
        readFirstPacket(payload);
        break;
      case READ_COLUMN_DEF:
        readColumnDef(payload);
        break;
      case READ_COLUMN_EOF:
        readColumnEof(payload);
        break;
      case READ_ROW:
        readRow(payload);
        break;
      default:
        throw new IllegalStateException(state.name());
    }

    return isReadCompleted();
  }

  ColumnCountPacketWrapper readFirstPacket(final MySQLPayload payload) {
    var wrapper = ColumnCountPacketWrapper.newInstance(payload);
    if (wrapper.isOkPacket()) {
      handleOkPacket((OKPacket) wrapper.getPacket());
    } else if (wrapper.isErrPacket()) {
      handleErrPacket((ErrPacket) wrapper.getPacket());
    } else {
      columnCount = ((ColumnCountPacket) wrapper.getPacket()).getCount();
      assert columnCount > 0 : "Column count must be greater than 0.";
      state = State.READ_COLUMN_DEF;
      beforeReadColumnDefinitions();
    }

    return wrapper;
  }

  ColumnDefinitionPacketWrapper readColumnDef(final MySQLPayload payload) {
    var wrapper = ColumnDefinitionPacketWrapper.newInstance(payload, false);
    if (wrapper.isErrPacket()) {
      handleErrPacket((ErrPacket) wrapper.getPacket());
    } else {
      var packet = wrapper.getPacket();
      if (!(packet instanceof ColumnDefinition41Packet)) {
        throw new IllegalStateException(
            String.format("Got an unexpected packet. [%s]", wrapper.getPacketDescription()));
      }

      onColumnRead((ColumnDefinition41Packet) packet);
      readColumnCount++;
      if (readColumnCount >= columnCount) {
        afterReadColumnDefinitions();
        state = State.READ_COLUMN_EOF;
      }
    }

    return wrapper;
  }

  GenericPacketWrapper readColumnEof(final MySQLPayload payload) {
    var wrapper = GenericPacketWrapper.newInstance(payload);
    if (wrapper.isErrPacket()) {
      handleErrPacket((ErrPacket) wrapper.getPacket());
    } else {
      if (!wrapper.isEofPacket()) {
        throw new IllegalStateException(
            String.format("Got an unexpected packet. [%s]", wrapper.getPacketDescription()));
      }
      state = State.READ_ROW;
      beforeReadRows();
    }
    return wrapper;
  }

  TextResultSetRowPacketWrapper readRow(final MySQLPayload payload) {
    var wrapper = TextResultSetRowPacketWrapper.newInstance(payload, columnCount);
    if (wrapper.isErrPacket()) {
      handleErrPacket((ErrPacket) wrapper.getPacket());
    } else if (wrapper.isEofPacket()) {
      afterReadRows();
      handleEofPacket((EofPacket) wrapper.getPacket());
    } else if (wrapper.isOkPacket()) {
      afterReadRows();
      handleOkPacket((OKPacket) wrapper.getPacket());
    } else {
      onRowRead((TextResultSetRowPacket) wrapper.getPacket());
    }

    return wrapper;
  }

  protected void beforeReadColumnDefinitions() {}

  protected void afterReadColumnDefinitions() {}

  protected void beforeReadRows() {}

  protected void afterReadRows() {}

  private void handleErrPacket(final ErrPacket packet) {
    state = State.FAILED;
    onFailure(new BackendResultReadException(CustomErrorCode.newInstance(packet)));
  }

  private void handleEofPacket(final EofPacket packet) {
    state = State.READ_COMPLETED;
    onSuccess(
        new CommandResult(
            0, 0, packet.getStatusFlags(), packet.getWarnings(), null, getQueryResult()));
  }

  protected QueryResult getQueryResult() {
    return null;
  }

  private void handleOkPacket(final OKPacket packet) {
    state = State.READ_COMPLETED;
    onSuccess(CommandResult.newInstance(packet, getQueryResult()));
  }

  protected abstract void onColumnRead(final ColumnDefinition41Packet packet);

  protected abstract void onRowRead(final TextResultSetRowPacket packet);

  @Override
  public boolean isReadCompleted() {
    return state == State.READ_COMPLETED || state == State.FAILED;
  }

}
