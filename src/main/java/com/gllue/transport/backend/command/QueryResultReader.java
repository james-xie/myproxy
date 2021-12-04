package com.gllue.transport.backend.command;

import com.gllue.command.result.query.QueryResultMetaData;

public interface QueryResultReader extends CommandResultReader {
  enum State {
    READ_FIRST_PACKET,
    READ_COLUMN_DEF,
    READ_COLUMN_EOF,
    READ_ROW,
    READ_COMPLETED,
    FAILED,
  }

  QueryResultMetaData getQueryResultMetaData();
}
