package com.gllue.myproxy.command.handler.query.ddl.alter;

import static com.gllue.myproxy.common.util.SQLStatementUtils.isDataTypeNameEquals;
import static com.gllue.myproxy.common.util.SQLStatementUtils.isEncryptColumn;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.Decryptor;
import com.gllue.myproxy.command.handler.query.Encryptor;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.metadata.model.ColumnType;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class EncryptColumnProcessor {
  private final Encryptor encryptor;
  private final Decryptor decryptor;
  final List<EncryptColumnInfo> encryptColumns = new ArrayList<>();
  final List<EncryptColumnInfo> decryptColumns = new ArrayList<>();

  public void addEncryptColumn(final EncryptColumnInfo columnInfo) {
    this.encryptColumns.add(columnInfo);
  }

  public void addDecryptColumn(final EncryptColumnInfo columnInfo) {
    this.decryptColumns.add(columnInfo);
  }

  void validateEncryptColumnDefinition(SQLColumnDefinition columnDef) {
    assert isEncryptColumn(columnDef);

    if (columnDef.getDefaultExpr() != null) {
      throw new BadSQLException(
          "Cannot set default expression on the encrypt column. [%s]", columnDef.getColumnName());
    }
  }

  void ensureSourceColumnType(ColumnType columnType) {
    if (columnType != ColumnType.VARCHAR) {
      throw new BadSQLException(
          "Cannot change column type [%s] to encrypt column.", columnType.name());
    }
  }

  void ensureTargetColumnType(SQLDataType dataType) {
    if (!isDataTypeNameEquals(dataType, ColumnType.VARCHAR)) {
      throw new BadSQLException(
          "Cannot change encrypt column to column type [%s].", dataType.getName());
    }
  }

  boolean isAddEncryptColumn(SQLAlterTableItem item) {
    if (item instanceof SQLAlterTableAddColumn) {
      var columns = ((SQLAlterTableAddColumn) item).getColumns();
      return !columns.isEmpty() && isEncryptColumn(columns.get(0));
    }
    return false;
  }

  boolean isModifyToEncryptColumn(SQLAlterTableItem item) {
    if (item instanceof MySqlAlterTableModifyColumn) {
      return isEncryptColumn(((MySqlAlterTableModifyColumn) item).getNewColumnDefinition());
    }
    return false;
  }

  boolean isChangeToEncryptColumn(SQLAlterTableItem item) {
    if (item instanceof MySqlAlterTableChangeColumn) {
      return isEncryptColumn(((MySqlAlterTableChangeColumn) item).getNewColumnDefinition());
    }
    return false;
  }

  SQLAlterTableAddColumn updateEncryptToVarbinary(final SQLAlterTableAddColumn item) {
    var columns = item.getColumns();
    var columnDef = columns.get(0);
    var newColumnDef = SQLStatementUtils.updateEncryptToVarbinary(columnDef);

    var newItem = new SQLAlterTableAddColumn();
    newItem.addColumn(newColumnDef);
    if (item.getFirstColumn() != null) {
      newItem.setFirstColumn(item.getFirstColumn());
    }
    if (item.getAfterColumn() != null) {
      newItem.setAfterColumn(item.getAfterColumn());
    }
    return newItem;
  }

  MySqlAlterTableModifyColumn updateEncryptToVarbinary(final MySqlAlterTableModifyColumn item) {
    var newItem = new MySqlAlterTableModifyColumn();
    var newColumnDef = SQLStatementUtils.updateEncryptToVarbinary(item.getNewColumnDefinition());
    newItem.setNewColumnDefinition(newColumnDef);
    if (item.getFirstColumn() != null) {
      newItem.setFirstColumn(item.getFirstColumn());
    }
    if (item.getAfterColumn() != null) {
      newItem.setAfterColumn(item.getAfterColumn());
    }
    return newItem;
  }

  MySqlAlterTableChangeColumn updateEncryptToVarbinary(final MySqlAlterTableChangeColumn item) {
    var newItem = new MySqlAlterTableChangeColumn();
    newItem.setColumnName(item.getColumnName());
    var newColumnDef = SQLStatementUtils.updateEncryptToVarbinary(item.getNewColumnDefinition());
    newItem.setNewColumnDefinition(newColumnDef);
    if (item.getFirstColumn() != null) {
      newItem.setFirstColumn(item.getFirstColumn());
    }
    if (item.getAfterColumn() != null) {
      newItem.setAfterColumn(item.getAfterColumn());
    }
    return newItem;
  }

  String temporaryColumnName(final String columnName) {
    return "$tmp_" + columnName;
  }

  SQLColumnDefinition newTemporaryVarbinaryColumnDef(
      SQLColumnDefinition columnDef, String tmpColumnName) {
    var newColumnDef = columnDef.clone();
    newColumnDef.setName(quoteName(tmpColumnName));
    newColumnDef.getDataType().setName(ColumnType.VARBINARY.name());
    return newColumnDef;
  }

  SQLColumnDefinition newTemporaryVarcharColumnDef(
      SQLColumnDefinition columnDef, String tmpColumnName) {
    var newColumnDef = columnDef.clone();
    newColumnDef.setName(quoteName(tmpColumnName));
    newColumnDef.getDataType().setName(ColumnType.VARCHAR.name());
    return newColumnDef;
  }

  SQLAlterTableAddColumn alterTableAddTemporaryColumnItem(
      MySqlAlterTableModifyColumn item, SQLColumnDefinition tmpColumn) {
    var newItem = new SQLAlterTableAddColumn();
    newItem.addColumn(tmpColumn);
    if (item.getFirstColumn() != null) {
      newItem.setFirstColumn(item.getFirstColumn());
    }
    if (item.getAfterColumn() != null) {
      newItem.setAfterColumn(item.getAfterColumn());
    }
    return newItem;
  }

  SQLAlterTableAddColumn alterTableAddTemporaryColumnItem(
      MySqlAlterTableChangeColumn item, SQLColumnDefinition tmpColumn) {
    var newItem = new SQLAlterTableAddColumn();
    newItem.addColumn(tmpColumn);
    if (item.getFirstColumn() != null) {
      newItem.setFirstColumn(item.getFirstColumn());
    }
    if (item.getAfterColumn() != null) {
      newItem.setAfterColumn(item.getAfterColumn());
    }
    return newItem;
  }

  boolean shouldDoEncryptOrDecrypt() {
    return !encryptColumns.isEmpty() || !decryptColumns.isEmpty();
  }

  String generateUpdateSqlToEncryptAndDecryptData(String tableName) {
    assert shouldDoEncryptOrDecrypt();

    var sqlPrefix = String.format("UPDATE `%s` SET ", tableName);
    var setItems = new ArrayList<String>();
    for (var columnInfo : encryptColumns) {
      var encryptExpr = encryptor.encryptExpr(quoteName(columnInfo.oldColumn));
      setItems.add(String.format("`%s` = %s", columnInfo.temporaryColumn, encryptExpr));
    }
    for (var columnInfo : decryptColumns) {
      var decryptExpr = decryptor.decryptExpr(quoteName(columnInfo.oldColumn));
      setItems.add(String.format("`%s` = %s", columnInfo.temporaryColumn, decryptExpr));
    }
    return sqlPrefix + String.join(", ", setItems);
  }

  String generateAlterSqlToRenameTemporaryColumn(String tableName) {
    assert shouldDoEncryptOrDecrypt();

    var sqlPrefix = String.format("ALTER TABLE `%s` ", tableName);
    var alterItems = new ArrayList<String>();
    var allColumns = new ArrayList<>(encryptColumns);
    allColumns.addAll(decryptColumns);
    for (var columnInfo : allColumns) {
      var columnDef = columnInfo.columnDefinition.clone();
      columnDef.setName(quoteName(columnInfo.newColumn));
      alterItems.add(String.format("DROP COLUMN `%s`", columnInfo.oldColumn));
      alterItems.add(String.format("CHANGE COLUMN `%s` %s", columnInfo.temporaryColumn, columnDef));
    }
    return sqlPrefix + String.join(", ", alterItems);
  }
}
