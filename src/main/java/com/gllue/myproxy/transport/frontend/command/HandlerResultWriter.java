package com.gllue.myproxy.transport.frontend.command;

import static com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet.CATALOG;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.constant.MySQLStatusFlag;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnCountPacket;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HandlerResultWriter implements CommandResultWriter {
  private final HandlerResult handlerResult;

  private int getStatusFlags(Connection connection) {
    int statusFlags = 0;
    if (connection.isAutoCommit()) {
      statusFlags |= MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue();
    }
    return statusFlags;
  }

  private void writeOk(Connection connection) {
    connection.writeAndFlush(
        new OKPacket(
            handlerResult.getAffectedRows(),
            handlerResult.getLastInsertId(),
            getStatusFlags(connection),
            handlerResult.getWarnings(),
            ""));
  }

  private void writeEof(Connection connection) {
    connection.write(new EofPacket(handlerResult.getWarnings(), getStatusFlags(connection)));
  }

  private void writeColumns(Connection connection, QueryResultMetaData metaData) {
    connection.write(new ColumnCountPacket(metaData.getColumnCount()));
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      connection.write(
          new ColumnDefinition41Packet(
              CATALOG,
              metaData.getSchemaName(i),
              metaData.getTableLabel(i),
              metaData.getTableName(i),
              metaData.getColumnLabel(i),
              metaData.getColumnName(i),
              MySQLServerInfo.DEFAULT_CHARSET,
              metaData.getColumnLength(i),
              metaData.getColumnType(i).getValue(),
              metaData.getColumnFlags(i),
              metaData.getDecimals(i),
              null));
    }
  }

  private void writeRows(Connection connection, QueryResult queryResult) {
    var metaData = queryResult.getMetaData();
    var columnCount = metaData.getColumnCount();

    while (queryResult.next()) {
      var row = new byte[columnCount][];
      for (int i = 0; i < columnCount; i++) {
        row[i] = queryResult.getValue(i);
      }
      connection.write(new TextResultSetRowPacket(row));
    }
  }

  private void writeTextResult(Connection connection) {
    var queryResult = handlerResult.getQueryResult();
    writeColumns(connection, queryResult.getMetaData());
    writeEof(connection);
    writeRows(connection, queryResult);
    writeEof(connection);
    connection.flush();
  }

  @Override
  public void write(Connection connection) {
    if (handlerResult.getQueryResult() == null) {
      writeOk(connection);
    } else {
      writeTextResult(connection);
    }
  }
}
