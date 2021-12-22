package com.gllue.myproxy.transport.backend.command;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DirectTransferFieldListResultReaderTest extends BaseCommandResultReaderTest {

  CommandResultReader prepareReader() {
    var reader = new DirectTransferFieldListResultReader(frontendConnection);
    reader.bindConnection(backendConnection);
    return reader;
  }

  @Test
  public void testReadEof() {
    var reader = prepareReader();
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
          }
        });
    var readCompleted = reader.read(packetToPayload(new EofPacket()));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_SUCCEED, executionState.get());
    assertThat(frontendChannel.readOutbound(), instanceOf(EofPacket.class));
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
    assertThat(frontendChannel.readOutbound(), instanceOf(ErrPacket.class));
  }

  @Test
  public void testReadColumns() {
    var reader = prepareReader();
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
          }
        });

    boolean readCompleted;
    String[] columnNames = new String[10];
    for (int i = 0; i < columnNames.length; i++) {
      var name = "col_" + i;
      columnNames[i] = name;
      readCompleted = reader.read(packetToPayload(newColumnDefPacket(name)));
      assertFalse(readCompleted);
    }

    readCompleted = reader.read(packetToPayload(new EofPacket()));
    assertTrue(readCompleted);

    reader.fireReadCompletedEvent();

    assertEquals(CallbackExecutionState.EXECUTION_SUCCEED, executionState.get());

    for (String columnName : columnNames) {
      var packet = frontendChannel.readOutbound();
      assertThat(packet, instanceOf(ColumnDefinition41Packet.class));
      var columnDefPacket = (ColumnDefinition41Packet) packet;
      assertEquals(columnName, columnDefPacket.getName());
    }
    assertThat(frontendChannel.readOutbound(), instanceOf(EofPacket.class));
  }

  @Test
  public void testReadColumnsWithError() {
    var reader = prepareReader();
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
          }
        });

    boolean readCompleted;
    for (int i = 0; i < 5; i++) {
      var name = "col_" + i;
      readCompleted = reader.read(packetToPayload(newColumnDefPacket(name)));
      assertFalse(readCompleted);
    }

    readCompleted =
        reader.read(packetToPayload(new ErrPacket(MySQLServerErrorCode.ER_NET_READ_ERROR)));
    assertTrue(readCompleted);

    reader.fireReadCompletedEvent();

    assertEquals(CallbackExecutionState.EXECUTION_FAILED, executionState.get());
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
}
