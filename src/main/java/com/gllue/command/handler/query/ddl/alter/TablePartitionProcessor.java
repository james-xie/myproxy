package com.gllue.command.handler.query.ddl.alter;

import static com.gllue.command.handler.query.TablePartitionHelper.generateExtensionTableName;
import static com.gllue.command.handler.query.TablePartitionHelper.newCreateExtensionTableStatement;
import static com.gllue.common.util.SQLStatementUtils.newAlterTableStatement;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.command.handler.CommandHandlerException;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.common.exception.BadColumnException;
import com.gllue.common.exception.ColumnExistsException;
import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigPropertyKey;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TablePartitionProcessor {

  private final PartitionTableMetaData table;
  private final Set<String> extensionColumns;
  private final int maxColumnsInExtensionTable;

  TablePartitionProcessor(
      final Configurations configurations,
      final PartitionTableMetaData table,
      final Map<SQLCommentAttributeKey, Object> attributes) {
    this.table = table;

    var extensionColumns = (String[]) attributes.get(SQLCommentAttributeKey.EXTENSION_COLUMNS);
    if (extensionColumns == null) {
      this.extensionColumns = new HashSet<>();
    } else {
      this.extensionColumns = new HashSet<>(Arrays.asList(extensionColumns));
    }

    int maxColumnsPerTable =
        configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE);
    double columnsAllocationWatermark =
        configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK);
    this.maxColumnsInExtensionTable = (int) (maxColumnsPerTable * columnsAllocationWatermark);
  }

  private void ensureColumnExists(String columnName) {
    if (!table.hasColumn(columnName)) {
      throw new BadColumnException(table.getName(), columnName);
    }
  }

  void validateStatement(SQLAlterTableStatement stmt) {
    if (stmt.isIgnore()) {
      throw new BadSQLException(
          "Partition table does not support the 'ALTER TABLE IGNORE' clause.");
    }

    if (stmt.getPartition() != null) {
      throw new BadSQLException("Partition table does not support the partition options.");
    }
  }

  int distributeAlterTableItem(SQLAlterTableItem item, int[] freeExtensionColumns) {
    int ordinalValue = 0;
    if (item instanceof SQLAlterTableAddColumn) {
      var columnDef = ((SQLAlterTableAddColumn) item).getColumns().get(0);
      var columnName = unquoteName(columnDef.getColumnName());
      if (extensionColumns.contains(columnName)) {
        return ordinalValue;
      }

      for (int i = 0; i < freeExtensionColumns.length; i++) {
        if (freeExtensionColumns[i] < maxColumnsInExtensionTable) {
          freeExtensionColumns[i]++;
          return i + 1;
        }
      }
      log.error("Found too many extension columns.");
    } else if (item instanceof MySqlAlterTableModifyColumn) {
      var columnDef = ((MySqlAlterTableModifyColumn) item).getNewColumnDefinition();
      var columnName = unquoteName(columnDef.getColumnName());
      ensureColumnExists(columnName);
      ordinalValue = table.getOrdinalValueByColumnName(columnName);
    } else if (item instanceof MySqlAlterTableChangeColumn) {
      var changeItem = (MySqlAlterTableChangeColumn) item;
      var columnName = unquoteName(changeItem.getColumnName().getSimpleName());
      ensureColumnExists(columnName);
      ordinalValue = table.getOrdinalValueByColumnName(columnName);
    } else if (item instanceof SQLAlterTableDropColumnItem) {
      var sqlName = ((SQLAlterTableDropColumnItem) item).getColumns().get(0);
      var columnName = unquoteName(sqlName.getSimpleName());
      ensureColumnExists(columnName);
      ordinalValue = table.getOrdinalValueByColumnName(columnName);
      if (ordinalValue > 0) {
        freeExtensionColumns[ordinalValue - 1]--;
      }
    } else {
      throw new BadSQLException("Unsupported alter table operation [%s].", item);
    }
    assert ordinalValue >= 0;
    return ordinalValue;
  }

  List<SQLAlterTableItem> extractTableOptionItems(List<SQLAlterTableItem> items) {
    return List.of();
  }

  List<MySqlCreateTableStatement> prepareCreateNewExtensionTables(
      List<SQLAlterTableItem> items,
      List<SQLAlterTableItem> tableOptionItems,
      Set<SQLAlterTableItem> excludeItems) {
    var columnsInNewExtensionTable = new ArrayList<SQLColumnDefinition>();
    var freeColumns = table.freeExtensionColumns(maxColumnsInExtensionTable);
    for (var item : items) {
      if (item instanceof SQLAlterTableAddColumn) {
        var columnDef = ((SQLAlterTableAddColumn) item).getColumns().get(0);
        var columnName = unquoteName(columnDef.getColumnName());
        if (table.hasColumn(columnName)) {
          throw new ColumnExistsException(columnName);
        }
        if (!extensionColumns.contains(columnName)) {
          continue;
        }

        if (freeColumns > 0) {
          freeColumns--;
        } else {
          excludeItems.add(item);
          columnsInNewExtensionTable.add(columnDef);
        }
      } else if (item instanceof SQLAlterTableDropColumnItem) {
        var sqlName = ((SQLAlterTableDropColumnItem) item).getColumns().get(0);
        var columnName = unquoteName(sqlName.getSimpleName());
        if (!table.hasColumn(columnName)) {
          throw new BadColumnException(table.getName(), columnName);
        }

        if (table.hasExtensionColumn(columnName)) {
          freeColumns++;
        }
      }
    }

    if (columnsInNewExtensionTable.isEmpty()) {
      return List.of();
    }

    var stmtList = new ArrayList<MySqlCreateTableStatement>();

    var itemCount = 0;
    var ordinalValue = table.nextOrdinalValue();
    var extTableName = generateExtensionTableName(table.getName(), ordinalValue);
    // todo: set table options.
    var createTableStmt = newCreateExtensionTableStatement(extTableName, new ArrayList<>(), null);
    for (var columnDef : columnsInNewExtensionTable) {
      createTableStmt.addColumn(columnDef);
      if (++itemCount >= maxColumnsInExtensionTable) {
        itemCount = 0;
        stmtList.add(createTableStmt);
        ordinalValue++;
        extTableName = generateExtensionTableName(table.getName(), ordinalValue);
        createTableStmt = newCreateExtensionTableStatement(extTableName, new ArrayList<>(), null);
      }
    }

    if (!createTableStmt.getColumnDefinitions().isEmpty()) {
      stmtList.add(createTableStmt);
    }
    return stmtList;
  }

  List<SQLAlterTableStatement> partitionStatement(
      List<SQLAlterTableItem> items,
      List<SQLAlterTableItem> tableOptionItems,
      Set<SQLAlterTableItem> excludeItems) {
    int count = table.getNumberOfTables();
    int[] freeExtensionColumns = new int[table.getNumberOfExtensionTables()];
    for (int i = 0; i < freeExtensionColumns.length; i++) {
      int columnCount = table.getTableByOrdinalValue(i + 1).getNumberOfColumns();
      freeExtensionColumns[i] = maxColumnsInExtensionTable - columnCount;
    }

    SQLAlterTableStatement[] stmtArray = new SQLAlterTableStatement[count];
    for (var item : items) {
      if (excludeItems.contains(item)) {
        continue;
      }

      int ordinalValue = distributeAlterTableItem(item, freeExtensionColumns);
      if (stmtArray[ordinalValue] == null) {
        var subTable = table.getTableByOrdinalValue(ordinalValue);
        stmtArray[ordinalValue] = newAlterTableStatement(subTable.getName(), false, null);
      }
      stmtArray[ordinalValue].addItem(item);
    }

    List<SQLAlterTableStatement> stmtList = new ArrayList<>();
    for (var stmtItem : stmtArray) {
      if (stmtItem != null) {
        stmtList.add(stmtItem);
      }
    }
    return stmtList;
  }
}
