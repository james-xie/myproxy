package com.gllue.command.handler.query;

import static com.gllue.common.util.SQLStatementUtils.newColumnDefinition;
import static com.gllue.common.util.SQLStatementUtils.newCreateTableStatement;
import static com.gllue.common.util.SQLStatementUtils.newKey;
import static com.gllue.common.util.SQLStatementUtils.newPrimaryKey;

import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.metadata.model.ColumnType;
import java.util.List;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TablePartitionHelper {
  public static final String EXTENSION_TABLE_PREFIX = "$e_";
  public static final String EXTENSION_TABLE_ID_COLUMN = "$_ext_id";
  public static final String KEY_FOR_EXTENSION_TABLE_ID_COLUMN = "$_key_for_ext_id";
  public static final ColumnType ID_COLUMN_TYPE = ColumnType.BIGINT;
  public static final String PRIMARY_KEY_COMMENT = "Primary key of the extension table.";

  public static final Set<String> AVAILABLE_TABLE_OPTION_KEYS =
      Set.of(
          "ENGINE",
          "AUTO_INCREMENT",
          "CHARSET",
          "CHARACTER SET",
          "COLLATE",
          "ROW_FORMAT",
          "COMPRESSION");

  public static String generatePrimaryTableName(final String tableName) {
    return tableName;
  }

  public static String generateExtensionTableName(final String tableName, final int ordinalValue) {
    return EXTENSION_TABLE_PREFIX + tableName.toLowerCase() + "_" + ordinalValue;
  }

  public static SQLPrimaryKey newExtensionTablePrimaryKey() {
    return newPrimaryKey(EXTENSION_TABLE_ID_COLUMN);
  }

  public static MySqlKey newKeyForExtensionTableIdColumn() {
    return newKey(KEY_FOR_EXTENSION_TABLE_ID_COLUMN, EXTENSION_TABLE_ID_COLUMN);
  }

  public static SQLColumnDefinition newExtensionTableIdColumn() {
    return newColumnDefinition(
        EXTENSION_TABLE_ID_COLUMN, ID_COLUMN_TYPE, false, null, PRIMARY_KEY_COMMENT);
  }

  public static boolean isExtensionTableIdColumn(final String columnName) {
    return EXTENSION_TABLE_ID_COLUMN.equals(columnName);
  }

  public static MySqlCreateTableStatement newCreateExtensionTableStatement(
      final String tableName,
      final List<SQLTableElement> columnDefs,
      final List<SQLTableElement> indices,
      final List<SQLTableElement> constraints,
      final List<SQLAssignItem> tableOptions) {
    columnDefs.add(newExtensionTableIdColumn());
    return newCreateTableStatement(
        tableName,
        columnDefs,
        newExtensionTablePrimaryKey(),
        indices,
        constraints,
        tableOptions,
        false);
  }

  public static MySqlCreateTableStatement newCreateExtensionTableStatement(
      final String tableName,
      final List<SQLTableElement> columnDefs,
      final List<SQLAssignItem> tableOptions) {
    columnDefs.add(newExtensionTableIdColumn());
    return newCreateTableStatement(
        tableName, columnDefs, newExtensionTablePrimaryKey(), null, null, tableOptions, false);
  }
}
