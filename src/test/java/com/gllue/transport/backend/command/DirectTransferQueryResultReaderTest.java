package com.gllue.transport.backend.command;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.constant.MySQLServerInfo;
import com.gllue.transport.backend.BackendResultReadException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.protocol.packet.generic.EofPacket;
import com.gllue.transport.protocol.packet.generic.ErrPacket;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import com.gllue.transport.protocol.packet.query.ColumnCountPacket;
import com.gllue.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.transport.protocol.packet.query.text.TextResultSetRowPacket;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DirectTransferQueryResultReaderTest extends BaseCommandResultReaderTest {

  CommandResultReader prepareReader() {
    return super.prepareReader(new DirectTransferQueryResultReader(frontendConnection));
  }

  @Test
  public void testReadOk() {
    var reader = prepareReader();
    var okPacket = new OKPacket(10, 11);
    var executionState = newExecutionStateRef();

    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
            assertEquals(okPacket.getAffectedRows(), result.getAffectedRows());
            assertEquals(okPacket.getLastInsertId(), result.getLastInsertId());
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }
        });

    var readCompleted = reader.read(packetToPayload(okPacket));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_SUCCEED, executionState.get());
  }

  @Test
  public void testReadErr() {
    var reader = prepareReader();
    var errPacket = new ErrPacket(MySQLServerErrorCode.ER_NET_READ_ERROR);
    var executionState = newExecutionStateRef();

    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
            Assert.assertThat(e, IsInstanceOf.instanceOf(BackendResultReadException.class));
            var exc = (BackendResultReadException) e;
            assertEquals(errPacket.getErrorCode(), exc.getErrorCode().getErrorCode());
          }
        });

    var readCompleted = reader.read(packetToPayload(errPacket));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_FAILED, executionState.get());
  }

  String[] prepareRows(final int columnCount) {
    String[] columnNames = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columnNames[i] = "col_" + i;
    }
    return columnNames;
  }

  byte[][][] prepareRows(final int rowCount, final int columnCount) {
    byte[][][] rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = new byte[columnCount][];
      for (int j = 0; j < columnCount; j++) {
        rows[i][j] = String.format("data[%d][%d]", i, j).getBytes();
      }
    }
    return rows;
  }

  @Test
  public void testReadRows() {
    var reader = prepareReader();
    var executionState = newExecutionStateRef();
    int columnCount = 3, rowCount = 10;
    var columnNames = prepareRows(columnCount);
    var rows = prepareRows(rowCount, columnCount);

    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }
        });

    var firstPacket = new ColumnCountPacket(columnCount);
    var readCompleted = reader.read(packetToPayload(firstPacket));
    assertFalse(readCompleted);

    for (var column : columnNames) {
      readCompleted = reader.read(packetToPayload(newColumnDefPacket(column)));
      assertFalse(readCompleted);
    }
    readCompleted = reader.read(packetToPayload(new EofPacket()));
    assertFalse(readCompleted);

    for (var row : rows) {
      readCompleted = reader.read(packetToPayload(newRowPacket(row)));
      assertFalse(readCompleted);
    }
    readCompleted = reader.read(packetToPayload(new EofPacket()));
    assertTrue(readCompleted);

    reader.fireReadCompletedEvent();

    assertEquals(CallbackExecutionState.EXECUTION_SUCCEED, executionState.get());

    var packet = frontendChannel.readOutbound();
    assertThat(packet, instanceOf(ColumnCountPacket.class));

    for (String columnName : columnNames) {
      packet = frontendChannel.readOutbound();
      assertThat(packet, instanceOf(ColumnDefinition41Packet.class));
      var columnDefPacket = (ColumnDefinition41Packet) packet;
      assertEquals(columnName, columnDefPacket.getName());
    }

    assertThat(frontendChannel.readOutbound(), instanceOf(EofPacket.class));

    for (var row : rows) {
      packet = frontendChannel.readOutbound();
      assertThat(packet, instanceOf(TextResultSetRowPacket.class));
      var rowPacket = (TextResultSetRowPacket) packet;
      assertArrayEquals(row, rowPacket.getRowData());
    }

    assertThat(frontendChannel.readOutbound(), instanceOf(EofPacket.class));
  }

  ColumnDefinition41Packet newColumnDefPacket(String name) {
    return new ColumnDefinition41Packet(
        "catalog",
        "db",
        "table",
        "orgtable",
        name,
        name,
        MySQLServerInfo.DEFAULT_CHARSET,
        10,
        MySQLColumnType.MYSQL_TYPE_STRING.getValue(),
        0,
        0,
        "null");
  }

  TextResultSetRowPacket newRowPacket(final byte[][] rowData) {
    return new TextResultSetRowPacket(rowData);
  }
}
