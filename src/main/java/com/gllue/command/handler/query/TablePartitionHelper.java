package com.gllue.command.handler.query;

import static com.gllue.common.util.SQLStatementUtils.newColumnDefinition;
import static com.gllue.common.util.SQLStatementUtils.newCreateTableStatement;
import static com.gllue.common.util.SQLStatementUtils.newKey;
import static com.gllue.common.util.SQLStatementUtils.newPrimaryKey;
import static com.gllue.common.util.SQLStatementUtils.quoteName;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import java.util.List;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TablePartitionHelper {
  public static final String EXTENSION_TABLE_PREFIX = "$e_";
  public static final String EXTENSION_TABLE_ID_COLUMN = "$_ext_id";
  public static final String QUOTED_EXTENSION_TABLE_ID_COLUMN =
      quoteName(EXTENSION_TABLE_ID_COLUMN);
  public static final String KEY_FOR_EXTENSION_TABLE_ID_COLUMN = "$_key_for_ext_id";
  public static final ColumnType ID_COLUMN_TYPE = ColumnType.BIGINT;
  public static final String PRIMARY_KEY_COMMENT = "Primary key of the extension table.";
  private static final String JOIN_TABLE_ALIAS_PREFIX = "$_ext_";

  private static final String SUB_QUERY_ALIAS = "$_sub_query";
  private static final String QUOTED_SUB_QUERY_ALIAS = String.format("`%s`", SUB_QUERY_ALIAS);

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

  public static SQLBinaryOpExpr newTableJoinCondition(
      final SQLExpr primaryTableAlias, final SQLExpr extensionTableAlias) {
    var left = new SQLPropertyExpr(primaryTableAlias, QUOTED_EXTENSION_TABLE_ID_COLUMN);
    var right = new SQLPropertyExpr(extensionTableAlias, QUOTED_EXTENSION_TABLE_ID_COLUMN);
    return new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right, DbType.mysql);
  }

  public static SQLTableSource constructSubQueryToFilter(
      SQLTableSource tableSource,
      SQLExpr owner,
      SQLExpr where,
      SQLOrderBy orderBy,
      SQLLimit limit) {
    var select = new MySqlSelectQueryBlock();
    var selectColumn = new SQLPropertyExpr(owner, QUOTED_EXTENSION_TABLE_ID_COLUMN);
    select.getSelectList().add(new SQLSelectItem(selectColumn));
    select.setFrom(tableSource);
    select.setWhere(where);
    select.setOrderBy(orderBy);
    select.setLimit(limit);
    var subQuery = new SQLSubqueryTableSource(new SQLSelect(select));
    subQuery.setAlias(QUOTED_SUB_QUERY_ALIAS);
    return subQuery;
  }
}
