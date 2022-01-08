package com.gllue.myproxy.command.handler.query.ddl.alter;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import java.util.Objects;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class EncryptColumnInfo {
  final String oldColumn;
  final String newColumn;
  final String temporaryColumn;
  final SQLColumnDefinition columnDefinition;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncryptColumnInfo that = (EncryptColumnInfo) o;
    return Objects.equals(oldColumn, that.oldColumn)
        && Objects.equals(newColumn, that.newColumn)
        && Objects.equals(temporaryColumn, that.temporaryColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldColumn, newColumn, temporaryColumn);
  }
}
