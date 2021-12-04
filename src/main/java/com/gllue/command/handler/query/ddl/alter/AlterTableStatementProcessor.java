package com.gllue.command.handler.query.ddl.alter;

import static com.gllue.common.util.SQLStatementUtils.isColumnDefinitionEquals;
import static com.gllue.common.util.SQLStatementUtils.isDataTypeNameEquals;
import static com.gllue.common.util.SQLStatementUtils.newDropColumnItem;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;
import static com.gllue.common.util.SQLStatementUtils.updateEncryptToVarbinary;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.gllue.command.handler.CommandHandlerException;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.common.exception.BadColumnException;
import com.gllue.common.exception.ColumnExistsException;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.TableMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AlterTableStatementProcessor {
  private final TableMetaData tableMetaData;
  private final Map<String, SQLColumnDefinition> columnsInDatabase;
  private final EncryptColumnProcessor encryptColumnProcessor;

  private final Set<String> alterColumns = new HashSet<>();

  private SQLAlterTableStatement newAlterTableStatement(
      String tableName, SQLAlterTableStatement stmt) {
    var newStmt = SQLStatementUtils.newAlterTableStatement(tableName, stmt.isIgnore(), null);
    if (stmt.isOnline()) {
      newStmt.setOnline(true);
    } else if (stmt.isOffline()) {
      newStmt.setOffline(true);
    }
    return newStmt;
  }

  private void ensureAlterItemNoDuplication(final String columnName) {
    if (alterColumns.contains(columnName)) {
      throw new BadSQLException("Each column can only support one alter item.");
    }
    alterColumns.add(columnName);
  }

  private MySqlAlterTableModifyColumn convertAlterTableChangeItemToModifyItem(
      final MySqlAlterTableChangeColumn changeItem) {
    var modifyItem = new MySqlAlterTableModifyColumn();
    modifyItem.setNewColumnDefinition(changeItem.getNewColumnDefinition());
    if (changeItem.getFirstColumn() != null) {
      modifyItem.setFirstColumn(changeItem.getFirstColumn());
    }
    if (changeItem.getAfterColumn() != null) {
      modifyItem.setAfterColumn(changeItem.getAfterColumn());
    }
    return modifyItem;
  }

  private List<SQLAlterTableItem> processAddColumn(SQLAlterTableAddColumn item) {
    if (item.getColumns().size() != 1) {
      throw new BadSQLException("SQLAlterTableAddColumn must contains only one column.");
    }

    var columnDef = item.getColumns().get(0);
    if (encryptColumnProcessor.isAddEncryptColumn(item)) {
      encryptColumnProcessor.validateEncryptColumnDefinition(columnDef);
      item = encryptColumnProcessor.updateEncryptToVarbinary(item);
    }
    var columnName = unquoteName(columnDef.getColumnName());
    ensureAlterItemNoDuplication(columnName);

    var result = new ArrayList<SQLAlterTableItem>();
    if (tableMetaData.hasColumn(columnName)) {
      throw new ColumnExistsException(columnName);
    } else if (columnsInDatabase.containsKey(columnName)) {
      // The column is found in the database, but not in the table metadata.
      if (isColumnDefinitionEquals(columnDef, columnsInDatabase.get(columnName))) {
        return result;
      }
      result.add(newDropColumnItem(columnName));
    }
    result.add(item);
    return result;
  }

  private List<SQLAlterTableItem> processModifyVarcharToEncryptColumn(
      MySqlAlterTableModifyColumn item, String columnName) {
    var columnDef = item.getNewColumnDefinition();
    encryptColumnProcessor.validateEncryptColumnDefinition(columnDef);
    encryptColumnProcessor.ensureSourceColumnType(tableMetaData.getColumn(columnName).getType());

    // The column has already modified to the encrypt column.
    var columnDefInDB = columnsInDatabase.get(columnName);
    if (isDataTypeNameEquals(columnDefInDB.getDataType(), ColumnType.VARBINARY)) {
      var varbinaryColumnDef = updateEncryptToVarbinary(columnDef);
      if (isColumnDefinitionEquals(columnDefInDB, varbinaryColumnDef)) {
        return List.of();
      }

      var newItem = new MySqlAlterTableModifyColumn();
      newItem.setNewColumnDefinition(varbinaryColumnDef);
      if (item.getFirstColumn() != null) {
        newItem.setFirstColumn(item.getFirstColumn());
      }
      if (item.getAfterColumn() != null) {
        newItem.setAfterColumn(item.getAfterColumn());
      }
      return List.of(newItem);
    }

    var result = new ArrayList<SQLAlterTableItem>();
    var tmpColumnName = encryptColumnProcessor.temporaryColumnName(columnName);
    var tmpColumn = encryptColumnProcessor.newTemporaryVarbinaryColumnDef(columnDef, tmpColumnName);
    // The temporary encrypt column has already added to the table schema.
    if (columnsInDatabase.containsKey(tmpColumnName)) {
      if (isColumnDefinitionEquals(tmpColumn, columnsInDatabase.get(tmpColumnName))) {
        return List.of();
      }
      result.add(newDropColumnItem(tmpColumnName));
    }

    result.add(encryptColumnProcessor.alterTableAddTemporaryColumnItem(item, tmpColumn));
    encryptColumnProcessor.encryptColumns.add(
        new EncryptColumnInfo(columnName, columnName, tmpColumnName, tmpColumn));
    return result;
  }

  private List<SQLAlterTableItem> processModifyEncryptToVarcharColumn(
      MySqlAlterTableModifyColumn item, String columnName) {
    var columnDef = item.getNewColumnDefinition();
    encryptColumnProcessor.ensureTargetColumnType(columnDef.getDataType());

    // The column has already modified to the encrypt column.
    var columnDefInDB = columnsInDatabase.get(columnName);
    if (isDataTypeNameEquals(columnDefInDB.getDataType(), ColumnType.VARCHAR)) {
      if (isColumnDefinitionEquals(columnDefInDB, columnDef)) {
        return List.of();
      }
      return List.of(item);
    }

    var result = new ArrayList<SQLAlterTableItem>();
    var tmpColumnName = encryptColumnProcessor.temporaryColumnName(columnName);
    var tmpColumn = encryptColumnProcessor.newTemporaryVarcharColumnDef(columnDef, tmpColumnName);
    // The temporary encrypt column has already added to the table schema.
    if (columnsInDatabase.containsKey(tmpColumnName)) {
      if (isColumnDefinitionEquals(tmpColumn, columnsInDatabase.get(tmpColumnName))) {
        return List.of();
      }
      result.add(newDropColumnItem(tmpColumnName));
    }

    result.add(encryptColumnProcessor.alterTableAddTemporaryColumnItem(item, tmpColumn));
    encryptColumnProcessor.encryptColumns.add(
        new EncryptColumnInfo(columnName, columnName, tmpColumnName, tmpColumn));
    return result;
  }

  private List<SQLAlterTableItem> processModifyColumn(MySqlAlterTableModifyColumn item) {
    var columnName = unquoteName(item.getNewColumnDefinition().getColumnName());
    ensureAlterItemNoDuplication(columnName);

    if (!tableMetaData.hasColumn(columnName)) {
      throw new BadColumnException(tableMetaData.getName(), columnName);
    }
    // The column is found in the table metadata, but not in the table schema.
    if (!columnsInDatabase.containsKey(columnName)) {
      log.warn(
          "Table metadata is not consistent with the table schema in the database. [{}.{}]",
          tableMetaData.getName(),
          columnName);
      return List.of();
    }

    // The column has already modified, ignore it.
    var columnDef = item.getNewColumnDefinition();
    var isEncryptColumn = tableMetaData.getColumn(columnName).getType() == ColumnType.ENCRYPT;
    var isModifyToEncryptColumn = encryptColumnProcessor.isModifyToEncryptColumn(item);
    if (isEncryptColumn || isModifyToEncryptColumn) {
      if (isEncryptColumn && isModifyToEncryptColumn) {
        item = encryptColumnProcessor.updateEncryptToVarbinary(item);
      } else if (isModifyToEncryptColumn) {
        return processModifyVarcharToEncryptColumn(item, columnName);
      } else {
        return processModifyEncryptToVarcharColumn(item, columnName);
      }
    }

    if (isColumnDefinitionEquals(columnsInDatabase.get(columnName), columnDef)) {
      return List.of();
    }
    return List.of(item);
  }

  private List<SQLAlterTableItem> processChangeVarcharToEncryptColumn(
      MySqlAlterTableChangeColumn item, String oldColumnName, String newColumnName) {
    var columnDef = item.getNewColumnDefinition();
    encryptColumnProcessor.validateEncryptColumnDefinition(columnDef);
    encryptColumnProcessor.ensureSourceColumnType(tableMetaData.getColumn(oldColumnName).getType());

    // The column has already changed to the encrypt column.
    if (!columnsInDatabase.containsKey(oldColumnName)
        && columnsInDatabase.containsKey(newColumnName)) {
      var columnDefInDB = columnsInDatabase.get(newColumnName);
      var varbinaryColumnDef = updateEncryptToVarbinary(columnDef);
      if (isColumnDefinitionEquals(columnDefInDB, varbinaryColumnDef)) {
        return List.of();
      } else if (!isDataTypeNameEquals(columnDefInDB.getDataType(), ColumnType.VARBINARY)) {
        log.error(
            "Failed to change varchar column to encrypt column, "
                + "because the old column [{}] does not exists and the new column [{}] is not a varbinary column.",
            oldColumnName,
            newColumnName);
        throw new ColumnExistsException(newColumnName);
      }

      var newItem = new MySqlAlterTableModifyColumn();
      newItem.setNewColumnDefinition(varbinaryColumnDef);
      if (item.getFirstColumn() != null) {
        newItem.setFirstColumn(item.getFirstColumn());
      }
      if (item.getAfterColumn() != null) {
        newItem.setAfterColumn(item.getAfterColumn());
      }
      return List.of(newItem);
    }

    var result = new ArrayList<SQLAlterTableItem>();
    var tmpColumnName = encryptColumnProcessor.temporaryColumnName(newColumnName);
    var tmpColumn = encryptColumnProcessor.newTemporaryVarbinaryColumnDef(columnDef, tmpColumnName);
    // The temporary encrypt column has already added to the table schema.
    if (columnsInDatabase.containsKey(tmpColumnName)) {
      if (isColumnDefinitionEquals(tmpColumn, columnsInDatabase.get(tmpColumnName))) {
        return List.of();
      }
      result.add(newDropColumnItem(tmpColumnName));
    }

    result.add(encryptColumnProcessor.alterTableAddTemporaryColumnItem(item, tmpColumn));
    encryptColumnProcessor.encryptColumns.add(
        new EncryptColumnInfo(oldColumnName, newColumnName, tmpColumnName, tmpColumn));
    return result;
  }

  private List<SQLAlterTableItem> processChangeEncryptToVarcharColumn(
      MySqlAlterTableChangeColumn item, String oldColumnName, String newColumnName) {
    var columnDef = item.getNewColumnDefinition();
    encryptColumnProcessor.ensureTargetColumnType(columnDef.getDataType());

    // The column has already changed to the encrypt column.
    if (!columnsInDatabase.containsKey(oldColumnName)
        && columnsInDatabase.containsKey(newColumnName)) {
      var columnDefInDB = columnsInDatabase.get(newColumnName);
      if (isColumnDefinitionEquals(columnDefInDB, columnDef)) {
        return List.of();
      } else if (!isDataTypeNameEquals(columnDefInDB.getDataType(), ColumnType.VARCHAR)) {
        log.error(
            "Failed to change encrypt column to varchar column, "
                + "because the old column [{}] does not exists and the new column [{}] is not a varchar column.",
            oldColumnName,
            newColumnName);
        throw new ColumnExistsException(newColumnName);
      }
      return List.of(item);
    }

    var result = new ArrayList<SQLAlterTableItem>();
    var tmpColumnName = encryptColumnProcessor.temporaryColumnName(newColumnName);
    var tmpColumn = encryptColumnProcessor.newTemporaryVarcharColumnDef(columnDef, tmpColumnName);
    // The temporary encrypt column has already added to the table schema.
    if (columnsInDatabase.containsKey(tmpColumnName)) {
      if (isColumnDefinitionEquals(tmpColumn, columnsInDatabase.get(tmpColumnName))) {
        return List.of();
      }
      result.add(newDropColumnItem(tmpColumnName));
    }

    result.add(encryptColumnProcessor.alterTableAddTemporaryColumnItem(item, tmpColumn));
    encryptColumnProcessor.encryptColumns.add(
        new EncryptColumnInfo(oldColumnName, newColumnName, tmpColumnName, tmpColumn));
    return result;
  }

  private List<SQLAlterTableItem> processChangeColumn(MySqlAlterTableChangeColumn item) {
    var oldColumnName = unquoteName(item.getColumnName().getSimpleName());
    var newColumnName = unquoteName(item.getNewColumnDefinition().getColumnName());
    if (oldColumnName.equals(newColumnName)) {
      return processModifyColumn(convertAlterTableChangeItemToModifyItem(item));
    }

    ensureAlterItemNoDuplication(oldColumnName);
    ensureAlterItemNoDuplication(newColumnName);

    if (!tableMetaData.hasColumn(oldColumnName)) {
      throw new BadColumnException(tableMetaData.getName(), oldColumnName);
    } else if (tableMetaData.hasColumn(newColumnName)) {
      throw new ColumnExistsException(newColumnName);
    }

    var columnDef = item.getNewColumnDefinition();

    // Neither old nor new column exists, maybe dropped before this operation.
    if (!columnsInDatabase.containsKey(oldColumnName)
        && !columnsInDatabase.containsKey(newColumnName)) {
      log.warn(
          "Table metadata is not consistent with the table schema in the database. [{}.{}]",
          tableMetaData.getName(),
          oldColumnName);
      return List.of();
    }

    var isEncryptColumn = tableMetaData.getColumn(oldColumnName).getType() == ColumnType.ENCRYPT;
    var isChangeToEncryptColumn = encryptColumnProcessor.isChangeToEncryptColumn(item);

    if (isEncryptColumn || isChangeToEncryptColumn) {
      if (isEncryptColumn && isChangeToEncryptColumn) {
        item = encryptColumnProcessor.updateEncryptToVarbinary(item);
      } else if (isChangeToEncryptColumn) {
        return processChangeVarcharToEncryptColumn(item, oldColumnName, newColumnName);
      } else {
        return processChangeEncryptToVarcharColumn(item, oldColumnName, newColumnName);
      }
    }

    if (!columnsInDatabase.containsKey(oldColumnName)) {
      // The column has already changed, ignore it.
      if (isColumnDefinitionEquals(columnsInDatabase.get(newColumnName), columnDef)) {
        return List.of();
      }

      // If the column definition is not equals to the column definition in table schema, we
      // just modify the column definition.
      var newItem = new MySqlAlterTableModifyColumn();
      newItem.setNewColumnDefinition(columnDef);
      return List.of(newItem);
    } else {
      // New column has already in the table schema, drop it and continue.
      if (columnsInDatabase.containsKey(newColumnName)) {
        log.error(
            "Table metadata is not consistent with the table schema in the database. [{}.{}]",
            tableMetaData.getName(),
            oldColumnName);
        return List.of(newDropColumnItem(newColumnName), item);
      }
    }
    return List.of(item);
  }

  private List<SQLAlterTableItem> processDropColumn(SQLAlterTableDropColumnItem item) {
    if (item.getColumns().size() != 1) {
      throw new BadSQLException("SQLAlterTableDropColumnItem must contains only one column.");
    }

    var columnDef = item.getColumns().get(0);
    var columnName = unquoteName(columnDef.getSimpleName());
    if (!tableMetaData.hasColumn(columnName)) {
      throw new BadColumnException(tableMetaData.getName(), columnName);
    }
    if (!columnsInDatabase.containsKey(columnName)) {
      return List.of();
    }
    return List.of(item);
  }

  SQLAlterTableStatement processStatement(final SQLAlterTableStatement stmt) {
    var tableName = tableMetaData.getName();
    var newStmt = newAlterTableStatement(tableName, stmt);

    for (var item : stmt.getItems()) {
      List<SQLAlterTableItem> newItems;
      if (item instanceof SQLAlterTableAddColumn) {
        newItems = processAddColumn((SQLAlterTableAddColumn) item);
      } else if (item instanceof MySqlAlterTableModifyColumn) {
        newItems = processModifyColumn((MySqlAlterTableModifyColumn) item);
      } else if (item instanceof MySqlAlterTableChangeColumn) {
        newItems = processChangeColumn((MySqlAlterTableChangeColumn) item);
      } else if (item instanceof SQLAlterTableDropColumnItem) {
        newItems = processDropColumn((SQLAlterTableDropColumnItem) item);
      } else {
        newItems = List.of(item);
      }

      for (var newItem : newItems) {
        newStmt.addItem(newItem);
      }
    }

    alterColumns.clear();
    return newStmt;
  }
}
