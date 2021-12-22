package com.gllue.myproxy.transport.backend.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.DefaultQueryRowsConsumerPipeline;
import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.command.result.query.QueryRowsConsumer;
import com.gllue.myproxy.command.result.query.QueryRowsConsumerPipeline;
import com.gllue.myproxy.command.result.query.Row;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnCountPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PipelineSupportedQueryResultReaderTest extends BaseCommandResultReaderTest {

  QueryRowsConsumerPipeline pipeline = new DefaultQueryRowsConsumerPipeline();

  CommandResultReader prepareReader() {
    return super.prepareReader(new PipelineSupportedQueryResultReader(pipeline));
  }

  @Test
  public void testReadOk() {
    var reader = prepareReader();
    var okPacket = new OKPacket(10, 11);
    var executionState = newExecutionStateRef();

    pipeline.addConsumer(
        new QueryRowsConsumer() {
          @Override
          public void begin(QueryResultMetaData metaData) {}

          @Override
          public void end() {}

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

          @Override
          public void accept(Row row) {}
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

    pipeline.addConsumer(
        new QueryRowsConsumer() {
          @Override
          public void begin(QueryResultMetaData metaData) {}

          @Override
          public void end() {}

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

          @Override
          public void accept(Row row) {}
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

  String[][] prepareRows(final int rowCount, final int columnCount) {
    String[][] rows = new String[rowCount][columnCount];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = new String[columnCount];
      for (int j = 0; j < columnCount; j++) {
        rows[i][j] = String.format("data[%d][%d]", i, j);
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
    List<String> stateCollector = new ArrayList<>();

    pipeline.addConsumer(
        new QueryRowsConsumer() {
          @Override
          public void begin(QueryResultMetaData metaData) {
            assertEquals(columnCount, metaData.getColumnCount());
            for (int i = 0; i < metaData.getColumnCount(); i++) {
              stateCollector.add(metaData.getColumnName(i));
            }
            stateCollector.add("begin");
          }

          @Override
          public void end() {
            stateCollector.add("end");
          }

          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }

          @Override
          public void accept(Row row) {
            for (var item: row.getRowData()) {
              stateCollector.add(new String(item));
            }
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

    List<String> expectedStates = new ArrayList<>(Arrays.asList(columnNames));
    expectedStates.add("begin");
    for (var row : rows) {
      expectedStates.addAll(Arrays.asList(row));
    }
    expectedStates.add("end");
    assertEquals(expectedStates, stateCollector);
  }

  @Test
  public void testReadRowsWithError() {
    var reader = prepareReader();
    var executionState = newExecutionStateRef();
    int columnCount = 3, rowCount = 10;
    var columnNames = prepareRows(columnCount);

    pipeline.addConsumer(
        new QueryRowsConsumer() {
          @Override
          public void begin(QueryResultMetaData metaData) {
            assertEquals(columnCount, metaData.getColumnCount());
          }

          @Override
          public void end() {}

          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }

          @Override
          public void accept(Row row) {}
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

  TextResultSetRowPacket newRowPacket(final String[] rowData) {
    return new TextResultSetRowPacket(rowData);
  }
}
