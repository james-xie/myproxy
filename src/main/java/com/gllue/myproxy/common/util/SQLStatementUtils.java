package com.gllue.myproxy.common.util;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLIndex;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyConstraint;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUniqueConstraint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SQLStatementUtils {

  private static final String ENCRYPT_COLUMN_TYPE = ColumnType.ENCRYPT.name();
  private static final String VARBINARY_COLUMN_TYPE = ColumnType.VARBINARY.name();

  public static String quoteName(final String name) {
    if (name.startsWith(ServerConstants.MYSQL_QUOTE_SYMBOL)
        || name.endsWith(ServerConstants.MYSQL_QUOTE_SYMBOL)) {
      return name;
    }
    return String.join(
        "", ServerConstants.MYSQL_QUOTE_SYMBOL, name, ServerConstants.MYSQL_QUOTE_SYMBOL);
  }

  public static String unquoteName(final String name) {
    int start = 0, end = name.length();
    if (name.startsWith(ServerConstants.MYSQL_QUOTE_SYMBOL)) {
      start++;
    }
    if (name.endsWith(ServerConstants.MYSQL_QUOTE_SYMBOL)) {
      end--;
    }
    return name.substring(start, end);
  }

  public static boolean columnIsNullable(final SQLColumnDefinition columnDef) {
    return columnDef.getConstraints().stream().noneMatch((x) -> x instanceof SQLNotNullConstraint);
  }

  public static String columnDefaultExpr(final SQLColumnDefinition columnDef) {
    var defaultExpr = columnDef.getDefaultExpr();
    if (defaultExpr == null || defaultExpr instanceof SQLNullExpr) {
      return null;
    }
    return defaultExpr.toString();
  }

  public static String visitColumn(final SQLSelectOrderByItem item) {
    var expr = item.getExpr();
    if (expr instanceof SQLIdentifierExpr) {
      return unquoteName(((SQLIdentifierExpr) expr).getSimpleName());
    }
    if (expr instanceof SQLMethodInvokeExpr) {
      return unquoteName(((SQLMethodInvokeExpr) expr).getMethodName());
    }

    throw new IllegalArgumentException("Failed to visit column.");
  }

  public static String visitColumn(final SQLName name) {
    return unquoteName(name.getSimpleName());
  }

  public static String[] visitColumns(List<SQLSelectOrderByItem> items) {
    String[] columnNames = new String[items.size()];
    int i = 0;
    for (var col : items) {
      columnNames[i++] = visitColumn(col);
    }
    return columnNames;
  }

  public static String[] getPrimaryKeyColumns(final SQLPrimaryKey key) {
    return visitColumns(key.getColumns());
  }

  public static String[] getIndexColumns(final SQLIndex index) {
    return visitColumns(index.getColumns());
  }

  public static String[] getUniqueKeyColumns(final SQLUniqueConstraint constraint) {
    return visitColumns(constraint.getColumns());
  }

  public static String[] getForeignKeyReferencingColumns(final SQLForeignKeyConstraint foreignKey) {
    var items = foreignKey.getReferencingColumns();
    String[] columnNames = new String[items.size()];
    int i = 0;
    for (var col : items) {
      columnNames[i++] = visitColumn(col);
    }
    return columnNames;
  }

  public static SQLPrimaryKey newPrimaryKey(String... columnNames) {
    var primaryKey = new MySqlPrimaryKey();
    var indexDefinition = primaryKey.getIndexDefinition();
    indexDefinition.setType("primary");
    indexDefinition.setKey(true);
    for (var columnName : columnNames) {
      indexDefinition
          .getColumns()
          .add(new SQLSelectOrderByItem(new SQLIdentifierExpr(quoteName(columnName))));
    }
    return primaryKey;
  }

  public static MySqlKey newKey(final String name, final String... columnNames) {
    var index = new MySqlKey();
    index.setName(quoteName(name));
    for (var columnName : columnNames) {
      index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr(quoteName(columnName))));
    }
    return index;
  }

  public static SQLColumnDefinition newColumnDefinition(
      final String columnName,
      final ColumnType columnType,
      final boolean nullable,
      final String defaultExpr,
      final String comment) {
    var columnDef = new SQLColumnDefinition();
    columnDef.setName(quoteName(columnName));
    columnDef.setDataType(new SQLDataTypeImpl(columnType.name()));
    if (!nullable) {
      columnDef.addConstraint(new SQLNotNullConstraint());
    }
    if (defaultExpr != null) {
      columnDef.setDefaultExpr(new SQLCharExpr(defaultExpr));
    }
    if (comment != null) {
      columnDef.setComment(comment);
    }
    return columnDef;
  }

  public static MySqlCreateTableStatement newCreateTableStatement(
      final String tableName,
      final List<SQLTableElement> columnDefs,
      final SQLTableElement primaryKey,
      final List<SQLTableElement> indices,
      final List<SQLTableElement> constraints,
      final List<SQLAssignItem> tableOptions,
      final boolean ifNotExists) {
    Preconditions.checkArgument(
        columnDefs != null && !columnDefs.isEmpty(), "Column definition cannot be null/empty.");
    Preconditions.checkArgument(primaryKey != null, "Primary key cannot be null.");

    var statement = new MySqlCreateTableStatement();
    statement.setTableName(quoteName(tableName));
    statement.setIfNotExiists(ifNotExists);

    var tableElements = statement.getTableElementList();
    tableElements.addAll(columnDefs);
    tableElements.add(primaryKey);
    if (indices != null) {
      tableElements.addAll(indices);
    }
    if (constraints != null) {
      tableElements.addAll(constraints);
    }
    if (tableOptions != null) {
      statement.getTableOptions().addAll(tableOptions);
    }
    return statement;
  }

  public static String toSQLString(final SQLStatement stmt) {
    return SQLUtils.toSQLString(stmt, stmt.getDbType());
  }

  public static SQLAlterTableStatement newAlterTableStatement(
      final String tableName, final boolean ignore, final List<SQLAlterTableItem> items) {
    Preconditions.checkNotNull(tableName, "tableName cannot be null.");

    var newStmt = new SQLAlterTableStatement();
    newStmt.setDbType(DbType.mysql);
    SQLExpr tableSource = SQLUtils.toSQLExpr(quoteName(tableName), DbType.mysql);
    newStmt.setTableSource(tableSource);
    newStmt.setIgnore(ignore);
    if (items != null) {
      for (var item : items) {
        newStmt.addItem(item);
      }
    }
    return newStmt;
  }

  public static SQLAlterTableDropColumnItem newDropColumnItem(final String columnName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(columnName), "columnName cannot be null/empty.");
    var item = new SQLAlterTableDropColumnItem();
    item.addColumn(new SQLIdentifierExpr(quoteName(columnName)));
    return item;
  }

  public static boolean isDataTypeNameEquals(
      final SQLDataType dataType, final ColumnType columnType) {
    return columnType.name().equalsIgnoreCase(dataType.getName());
  }

  public static boolean isColumnDefinitionEquals(
      final SQLColumnDefinition col1, final SQLColumnDefinition col2) {
    if (col1 == col2) {
      return true;
    }
    if (col1 == null || col2 == null) {
      return false;
    }

    if (!unquoteName(col1.getColumnName()).equals(unquoteName(col2.getColumnName()))) {
      return false;
    }

    if (!col1.getDataType().toString().equalsIgnoreCase(col2.getDataType().toString())) {
      return false;
    }
    var defaultExpr1 = col1.getDefaultExpr();
    var defaultExpr2 = col2.getDefaultExpr();
    if ((defaultExpr1 == null && defaultExpr2 != null)
        || (defaultExpr1 != null && defaultExpr2 == null)
        || (defaultExpr1 != null && !defaultExpr1.toString().equals(defaultExpr2.toString()))) {
      return false;
    }

    var collate1 = col1.getCollateExpr();
    var collate2 = col2.getCollateExpr();
    if ((collate1 == null && collate2 != null)
        || (collate1 != null && collate2 == null)
        || (collate1 != null && !collate1.toString().equalsIgnoreCase(collate2.toString()))) {
      return false;
    }

    if (col1.isAutoIncrement() != col2.isAutoIncrement()) {
      return false;
    }

    var comment1 = col1.getComment();
    var comment2 = col2.getComment();
    if ((comment1 == null && comment2 != null)
        || (comment1 != null && comment2 == null)
        || (comment1 != null && !comment1.toString().equals(comment2.toString()))) {
      return false;
    }

    var constraints1 =
        col1.getConstraints().stream().map(Object::toString).collect(Collectors.toSet());
    var constraints2 =
        col2.getConstraints().stream().map(Object::toString).collect(Collectors.toSet());
    return constraints1.equals(constraints2);
  }

  public static boolean isEncryptColumnType(final SQLDataType dataType) {
    return ENCRYPT_COLUMN_TYPE.equals(dataType.getName().toUpperCase());
  }

  public static boolean isVarbinaryColumnType(final SQLDataType dataType) {
    return VARBINARY_COLUMN_TYPE.equals(dataType.getName().toUpperCase());
  }

  public static boolean isEncryptColumn(final SQLColumnDefinition columnDef) {
    return isEncryptColumnType(columnDef.getDataType());
  }

  public static boolean isVarbinaryColumn(final SQLColumnDefinition columnDef) {
    return isVarbinaryColumnType(columnDef.getDataType());
  }

  public static SQLColumnDefinition updateEncryptToVarbinary(final SQLColumnDefinition columnDef) {
    Preconditions.checkArgument(
        isEncryptColumn(columnDef), "Illegal column type [%s].", columnDef.getDataType());

    var newColumnDef = columnDef.clone();
    newColumnDef.getDataType().setName(ColumnType.VARBINARY.name());
    return newColumnDef;
  }

  public static SQLColumnDefinition updateVarbinaryToEncrypt(final SQLColumnDefinition columnDef) {
    Preconditions.checkArgument(
        isVarbinaryColumn(columnDef), "Illegal column type [%s].", columnDef.getDataType());

    var newColumnDef = columnDef.clone();
    newColumnDef.getDataType().setName(ColumnType.ENCRYPT.name());
    return newColumnDef;
  }

  /**
   * Get table owner from the column property expression.
   *
   * @param propertyExpr property expression for the column
   * @return a string of table owner
   */
  public static String getTableOwner(final SQLPropertyExpr propertyExpr) {
    var expr = propertyExpr.getOwner();
    if (expr instanceof SQLIdentifierExpr) {
      return ((SQLIdentifierExpr) expr).getName();
    }
    if (expr instanceof SQLPropertyExpr) {
      return ((SQLPropertyExpr) expr).getSimpleName();
    }
    return null;
  }

  /**
   * Get schema owner from the column property expression.
   *
   * @param propertyExpr property expression for the column
   * @return a string of schema owner
   */
  public static String getSchemaOwner(final SQLPropertyExpr propertyExpr) {
    var expr = propertyExpr.getOwner();
    if (expr instanceof SQLPropertyExpr) {
      SQLExpr owner = ((SQLPropertyExpr) expr).getOwner();
      if (owner instanceof SQLIdentifierExpr) {
        return ((SQLIdentifierExpr) owner).getName();
      }

      if (owner instanceof SQLPropertyExpr) {
        return ((SQLPropertyExpr) owner).getSimpleName();
      }
    }
    return null;
  }

  /**
   * Get schema from the table source.
   *
   * @param tableSource table source in the sql statement.
   * @return a string of the schema name.
   */
  public static String getSchema(final SQLExprTableSource tableSource) {
    var schema = tableSource.getSchema();
    if (schema != null) {
      schema = unquoteName(schema);
    }
    return schema;
  }

  /**
   * Get table name from the table source.
   *
   * @param tableSource table source in the sql statement.
   * @return a string of the table name.
   */
  public static String getTableName(final SQLExprTableSource tableSource) {
    var tableName = tableSource.getTableName();
    if (tableName != null) {
      tableName = unquoteName(tableName);
    }
    return tableName;
  }

  public static String getAliasOrTableName(final SQLExprTableSource tableSource) {
    var alias = tableSource.getAlias();
    if (alias != null) {
      return unquoteName(alias);
    }
    return getTableName(tableSource);
  }

  public static void iterateSources(
      final SQLTableSource tableSource, Consumer<SQLTableSource> consumer) {
    if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      iterateSources(source.getLeft(), consumer);
      iterateSources(source.getRight(), consumer);
    } else {
      consumer.accept(tableSource);
    }
  }

  public static List<SQLTableSource> listTableSources(
      final SQLTableSource tableSource, Predicate<SQLTableSource> predicate) {
    var tableSources = new ArrayList<SQLTableSource>();
    iterateSources(
        tableSource,
        (x) -> {
          if (predicate.test(x)) {
            tableSources.add(x);
          }
        });
    return tableSources;
  }

  public static boolean anySubQueryExists(final SQLTableSource tableSource) {
    var exists = new AtomicBoolean();
    iterateSources(
        tableSource,
        (x) -> {
          if (x instanceof SQLSubqueryTableSource) {
            exists.set(true);
          }
        });
    return exists.get();
  }

  public static List<SQLSelectItem> newSQLSelectItems(
      final SQLExpr owner, List<String> columnNames) {
    return columnNames.stream()
        .map(x -> new SQLSelectItem(new SQLPropertyExpr(owner, quoteName(x))))
        .collect(Collectors.toList());
  }

  public static SQLExprTableSource newExprTableSource(
      final String schema, final String tableName, final String alias) {
    SQLExpr expr;
    if (schema == null) {
      expr = new SQLIdentifierExpr(quoteName(tableName));
    } else {
      expr = new SQLPropertyExpr(quoteName(schema), quoteName(tableName));
    }
    return new SQLExprTableSource(expr, quoteName(alias));
  }

  public static SQLSelectItem newSelectVersionItem(String alias) {
    Preconditions.checkArgument(alias != null, "alias cannot be null.");
    var expr = new SQLCharExpr(ServerConstants.getServerVersion());
    return new SQLSelectItem(expr, alias);
  }

  public static boolean isSQLEquals(final SQLStatement s1, final SQLStatement s2) {
    if (s1 == s2) {
      return true;
    } else if (s1 == null || s2 == null) {
      return false;
    } else if (s1.getClass() != s2.getClass()) {
      return false;
    }

    var sql1 = SQLStatementUtils.toSQLString(s1);
    var sql2 = SQLStatementUtils.toSQLString(s2);
    if (sql1.endsWith(";")) {
      sql1 = sql1.substring(0, sql1.length() - 1);
    }
    if (sql2.endsWith(";")) {
      sql2 = sql2.substring(0, sql2.length() - 1);
    }
    return sql1.equals(sql2);
  }

  public static boolean isSQLEquals(final String sql1, final String sql2) {
    final SQLParser sqlParser = new SQLParser();
    return isSQLEquals(sqlParser.parse(sql1), sqlParser.parse(sql2));
  }
}
