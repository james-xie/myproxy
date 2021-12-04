package com.gllue.command.handler.query.ddl.alter;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class EncryptColumnInfo {
  final String oldColumn;
  final String newColumn;
  final String temporaryColumn;
  final SQLColumnDefinition columnDefinition;
}
