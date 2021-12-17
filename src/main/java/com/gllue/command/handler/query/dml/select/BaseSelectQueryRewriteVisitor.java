package com.gllue.command.handler.query.dml.select;

import static com.gllue.command.handler.query.TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.command.handler.query.TablePartitionHelper.QUOTED_EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.common.util.SQLStatementUtils.getAliasOrTableName;
import static com.gllue.common.util.SQLStatementUtils.newExprTableSource;
import static com.gllue.common.util.SQLStatementUtils.newSQLSelectItems;
import static com.gllue.common.util.SQLStatementUtils.quoteName;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;
import static com.gllue.constant.ServerConstants.ALL_COLUMN_EXPR;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.common.exception.BadColumnException;
import com.gllue.common.exception.NoDatabaseException;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableType;
import com.gllue.metadata.model.TemporaryColumnMetaData;
import com.gllue.metadata.model.TemporaryTableMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BaseSelectQueryRewriteVisitor extends MySqlASTVisitorAdapter {
  private static final String JOIN_TABLE_ALIAS_PREFIX = "$ext_";

  protected final String defaultDatabase;
  private final TableScopeFactory tableScopeFactory;

  protected TableScope scope;
  protected boolean shouldRewriteQuery = false;
  protected boolean joinedExtensionTables = false;
  private Map<SQLExprTableSource, List<PreparedJoinExtensionTable>> preparedJoinExtensionTableMap;
  private final List<MySqlSelectQueryBlock> selectQueryBlocks = new ArrayList<>();

  @Getter private boolean queryChanged = false;

  private boolean inheritScope = true;
  private int aliasIndex = 0;
  private Set<String> affectedExtensionTables = null;
  private Stack<String> subQueryStack;

  @RequiredArgsConstructor
  static class PreparedJoinExtensionTable {
    final String tableName;
    final SQLTableSource tableSource;
    final SQLExpr joinCondition;
  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
    if (x.getAlias() == null) {
      throw new NoTableAliasException();
    }
    inheritScope = false;

    if (subQueryStack == null) {
      subQueryStack = new Stack<>();
    }
    subQueryStack.push(unquoteName(x.getAlias()));
    return true;
  }

  @Override
  public void endVisit(SQLSubqueryTableSource x) {
    subQueryStack.pop();
  }

  @Override
  public boolean visit(SQLUnionQueryTableSource x) {
    inheritScope = false;
    return true;
  }

  @Override
  public boolean visit(SQLExprTableSource x) {
    // disable visit table source property.
    return false;
  }

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    newScope(x.getFrom());
    rewriteSelectQuery(x);
    return true;
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    TemporaryTableMetaData temporaryTable = null;
    if (subQueryStack != null && !subQueryStack.empty() && scope.anyTablesInScope()) {
      var subQueryAlias = subQueryStack.peek();
      temporaryTable = resolveSubQuerySelectColumns(subQueryAlias, x.getSelectList());
    }

    exitScope();

    if (temporaryTable != null) {
      scope.addTable(defaultDatabase, temporaryTable.getName(), temporaryTable);
    }
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    if (!shouldRewriteQuery) {
      return false;
    }

    var columnName = unquoteName(x.getSimpleName());
    var column = scope.findColumnInScope(defaultDatabase, columnName);
    if (column != null) {
      checkForAffectedExtensionTables(column);
    }
    return false;
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    if (!shouldRewriteQuery) {
      return false;
    }

    var schema = getSchemaOwner(x);
    var tableOrAlias = getTableOwner(x);
    var table = scope.getTable(schema, tableOrAlias);
    var columnName = unquoteName(x.getName());
    if (table != null) {
      if (table.getType() == TableType.TEMPORARY) {
        var column = table.getColumn(columnName);
        if (column != null) {
          checkForAffectedExtensionTables(column);
        }
      } else {
        if (EXTENSION_TABLE_ID_COLUMN.equals(columnName)) {
          return false;
        } else if (!table.hasColumn(columnName)) {
          throw new BadColumnException(tableOrAlias, columnName);
        }
        if (table.getType() == TableType.PARTITION) {
          rewritePropertyOwnerForPartitionTable(
              x, (PartitionTableMetaData) table, schema, tableOrAlias, columnName);
        }
      }
    }
    return false;
  }

  protected void setQueryChanged() {
    queryChanged = true;
  }

  protected void newScope(SQLTableSource tableSource) {
    this.scope = tableScopeFactory.newTableScope(scope, inheritScope, tableSource);
    inheritScope = true;
  }

  protected void exitScope() {
    assert scope != null;
    this.scope = scope.getParent();
  }

  protected void rewriteSelectQuery(MySqlSelectQueryBlock x) {
    if (!shouldRewriteQuery) {
      shouldRewriteQuery = scope.anyTablesInScope();
    }

    var tableAliases = new ArrayList<Object>();
    var hasNestedQuery = collectTableAliases(x.getFrom(), tableAliases);
    if (hasNestedQuery) {
      shouldRewriteQuery = true;
    }

    if (shouldRewriteQuery) {
      rewriteSelectAllColumnExpr(x.getSelectList(), tableAliases);
      rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
      prepareJoinExtensionTables(x.getFrom());
      selectQueryBlocks.add(x);
    }
  }

  protected boolean collectTableAliases(SQLTableSource tableSource, List<Object> tableAliases) {
    var hasNestedQuery = false;
    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      if (source.getAlias() != null) {
        tableAliases.add(source.getAlias());
      } else {
        tableAliases.add(source.getExpr());
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      hasNestedQuery = collectTableAliases(source.getLeft(), tableAliases);
      hasNestedQuery |= collectTableAliases(source.getRight(), tableAliases);
    } else if (tableSource instanceof SQLUnionQueryTableSource) {
      var source = (SQLUnionQueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
      hasNestedQuery = true;
    } else if (tableSource instanceof SQLSubqueryTableSource) {
      var source = (SQLSubqueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
      hasNestedQuery = true;
    }

    return hasNestedQuery;
  }

  private int findAllColumnExpr(List<SQLSelectItem> items) {
    int i = 0;
    int index = -1;
    for (var item : items) {
      if (item.getExpr() instanceof SQLAllColumnExpr) {
        index = i;
        break;
      }
      i++;
    }
    return index;
  }

  private void rewriteSelectAllColumnExpr(List<SQLSelectItem> items, List<Object> tableAliases) {
    var index = findAllColumnExpr(items);
    if (index < 0) {
      return;
    }
    items.remove(index);

    var newItems = new ArrayList<SQLSelectItem>();
    for (var element : tableAliases) {
      if (element instanceof String) {
        newItems.add(new SQLSelectItem(new SQLPropertyExpr((String) element, ALL_COLUMN_EXPR)));
      } else if (element instanceof SQLExpr) {
        newItems.add(new SQLSelectItem(new SQLPropertyExpr((SQLExpr) element, ALL_COLUMN_EXPR)));
      }
    }

    items.addAll(index, newItems);
    setQueryChanged();
  }

  private List<SQLSelectItem> expandStarIntoTableColumns(
      List<SQLSelectItem> items, Map<Integer, SQLPropertyExpr> properties) {
    var newItems = new ArrayList<SQLSelectItem>();
    for (int i = 0; i < items.size(); i++) {
      var property = properties.get(i);
      if (property == null) {
        newItems.add(items.get(i));
        continue;
      }

      var schema = getSchemaOwner(property);
      var tableName = getTableOwner(property);
      var table = scope.getTable(schema, tableName);
      if (table == null) {
        newItems.add(items.get(i));
      } else {
        for (var column : table.getColumnNames()) {
          newItems.add(
              new SQLSelectItem(new SQLPropertyExpr(property.getOwner(), quoteName(column))));
        }
      }
    }
    return newItems;
  }

  protected void rewriteSingleTableSelectAllColumnExpr(List<SQLSelectItem> items) {
    Map<Integer, SQLPropertyExpr> properties = new HashMap<>();

    int i = 0;
    for (var item : items) {
      i++;
      if (!(item.getExpr() instanceof SQLPropertyExpr)) {
        continue;
      }
      var expr = (SQLPropertyExpr) item.getExpr();
      if (!ALL_COLUMN_EXPR.equals(expr.getName())) {
        continue;
      }

      properties.put(i - 1, expr);
    }

    if (properties.isEmpty()) {
      return;
    }

    var newItems = expandStarIntoTableColumns(items, properties);
    items.clear();
    items.addAll(newItems);
    setQueryChanged();
  }

  private String generateTableAlias() {
    return JOIN_TABLE_ALIAS_PREFIX + (aliasIndex++);
  }

  private SQLBinaryOpExpr newTableJoinCondition(
      final SQLExpr primaryTableAlias, final SQLExpr extensionTableAlias) {
    var left = new SQLPropertyExpr(primaryTableAlias, QUOTED_EXTENSION_TABLE_ID_COLUMN);
    var right = new SQLPropertyExpr(extensionTableAlias, QUOTED_EXTENSION_TABLE_ID_COLUMN);
    return new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right, DbType.mysql);
  }

  private boolean prepareJoinExtensionTables(
      String schema, SQLExprTableSource tableSource, PartitionTableMetaData table) {
    if (table.getNumberOfExtensionTables() == 0) {
      return false;
    }

    SQLExpr primaryTableAlias;
    if (tableSource.getAlias() != null) {
      primaryTableAlias = new SQLIdentifierExpr(tableSource.getAlias());
    } else {
      primaryTableAlias = tableSource.getExpr();
    }

    int i = 0;
    var extensionTables = table.getExtensionTables();
    var aliases = new String[extensionTables.length];
    for (var extTable : extensionTables) {
      var alias = generateTableAlias();
      aliases[i++] = alias;
      scope.addTable(schema, alias, extTable);

      var quoteAlias = quoteName(alias);
      var aliasExpr = new SQLIdentifierExpr(quoteAlias);
      addPreparedJoinExtensionTable(
          tableSource,
          new PreparedJoinExtensionTable(
              extTable.getName(),
              newExprTableSource(schema, extTable.getName(), quoteAlias),
              newTableJoinCondition(primaryTableAlias, aliasExpr)));
    }

    var aliasOrTableName = getAliasOrTableName(tableSource);
    scope.addExtensionTableAlias(schema, aliasOrTableName, aliases);
    return true;
  }

  private void addPreparedJoinExtensionTable(
      SQLExprTableSource tableSource, PreparedJoinExtensionTable table) {
    if (preparedJoinExtensionTableMap == null) {
      preparedJoinExtensionTableMap = new HashMap<>();
    }
    preparedJoinExtensionTableMap.computeIfAbsent(tableSource, k -> new ArrayList<>()).add(table);
  }

  /**
   * Convert the partition table source object to the join table source object. it just joins the
   * primary table and extension tables.
   */
  private SQLTableSource joinExtensionTables(
      SQLExprTableSource tableSource,
      List<PreparedJoinExtensionTable> preparedJoinTables,
      boolean joinAll) {
    SQLTableSource left = tableSource;
    for (var joinTable : preparedJoinTables) {
      if (joinAll
          || (affectedExtensionTables != null
              && affectedExtensionTables.contains(joinTable.tableName))) {
        var right = joinTable.tableSource;
        var condition = joinTable.joinCondition;
        left = new SQLJoinTableSource(left, JoinType.LEFT_OUTER_JOIN, right, condition);
      }
    }

    if (left != tableSource) {
      joinedExtensionTables = true;
      setQueryChanged();
    }
    return left;
  }

  protected boolean prepareJoinExtensionTables(SQLTableSource tableSource) {
    var hasExtensionTable = false;
    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      var schema = getSchema(source);
      var aliasOrTableName = getAliasOrTableName(source);
      var table = scope.getTable(schema, aliasOrTableName);
      if (table != null && table.getType() == TableType.PARTITION) {
        return prepareJoinExtensionTables(schema, source, (PartitionTableMetaData) table);
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      hasExtensionTable = prepareJoinExtensionTables(source.getLeft());
      hasExtensionTable |= prepareJoinExtensionTables(source.getRight());
    }
    return hasExtensionTable;
  }

  protected SQLTableSource joinExtensionTables(SQLTableSource tableSource) {
    return joinExtensionTables(tableSource, false);
  }

  protected SQLTableSource joinExtensionTables(SQLTableSource tableSource, boolean joinAll) {
    if (preparedJoinExtensionTableMap == null) {
      return tableSource;
    }

    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      if (preparedJoinExtensionTableMap.containsKey(source)) {
        return joinExtensionTables(source, preparedJoinExtensionTableMap.get(source), joinAll);
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      source.setLeft(joinExtensionTables(source.getLeft(), joinAll));
      source.setRight(joinExtensionTables(source.getRight(), joinAll));
    }
    return tableSource;
  }

  protected void joinExtensionTablesForSelectQueryBlocks() {
    if (preparedJoinExtensionTableMap != null) {
      for (var queryBlock : selectQueryBlocks) {
        queryBlock.setFrom(joinExtensionTables(queryBlock.getFrom()));
      }
    }
  }

  private TemporaryTableMetaData resolveSubQuerySelectColumns(
      String tableAlias, List<SQLSelectItem> items) {
    var shouldReplaceItems = false;
    var newItems = new ArrayList<SQLSelectItem>();
    var builder = new TemporaryTableMetaData.Builder().setName(tableAlias);
    for (var item : items) {
      var alias = item.getAlias();
      var expr = item.getExpr();

      String schema = defaultDatabase;
      String tableOrAlias = null;
      String columnName = null;
      if (expr instanceof SQLPropertyExpr) {
        var property = (SQLPropertyExpr) expr;
        schema = getSchemaOwner(property);
        tableOrAlias = getTableOwner(property);
        columnName = unquoteName(property.getName());

        if (ALL_COLUMN_EXPR.equals(columnName)) {
          var table = scope.getTable(schema, tableOrAlias);
          if (table == null) {
            throw new BadSQLException("Cannot resolve '*' in the sub-query select.");
          }
          var columnNames = table.getColumnNames();
          newItems.addAll(newSQLSelectItems(property.getOwner(), columnNames));
          for (var colName : columnNames) {
            builder.addColumnName(colName);
          }
          for (int colIndex = 0; colIndex < table.getNumberOfColumns(); colIndex++) {
            builder.addColumn(table.getColumn(colIndex));
          }

          shouldReplaceItems = true;
          continue;
        }
      } else if (expr instanceof SQLIdentifierExpr) {
        columnName = unquoteName(((SQLIdentifierExpr) expr).getSimpleName());
      }

      var nameOrAlias = alias != null ? alias : columnName;
      if (nameOrAlias != null) {
        builder.addColumnName(nameOrAlias);
      } else {
        builder.addColumnName(expr.toString());
      }

      if (columnName != null) {
        var column = findColumnInScope(scope, schema, tableOrAlias, columnName);
        if (column != null) {
          builder.addColumn(nameOrAlias, column);
        }
      }

      newItems.add(item);
    }

    if (shouldReplaceItems) {
      items.clear();
      items.addAll(newItems);
      setQueryChanged();
    }
    if (builder.anyColumnExists()) {
      return builder.build();
    }
    return null;
  }

  /** Wrap the column expression with AES_DECRYPT() function. */
  protected SQLExpr decryptColumn(String encryptKey, SQLExpr columnExpr) {
    var columnStr = columnExpr.toString();
    var decryptStr = String.format("AES_DECRYPT(%s, '%s')", columnStr, encryptKey);
    return new SQLIdentifierExpr(decryptStr);
  }

  /** Wrap the column expression with DES_DECRYPT() function. */
  protected SQLExpr encryptColumn(String encryptKey, SQLExpr columnExpr) {
    var columnStr = columnExpr.toString();
    var encryptStr = String.format("AES_ENCRYPT(%s, '%s')", columnStr, encryptKey);
    return new SQLIdentifierExpr(encryptStr);
  }

  private void rewritePropertyOwnerForPartitionTable(
      SQLPropertyExpr property,
      PartitionTableMetaData table,
      String schema,
      String tableName,
      String column) {
    var ordinalValue = table.getOrdinalValueByColumnName(column);
    // This column in the primary table, ignore it.
    if (ordinalValue == 0) {
      return;
    }

    var aliases = scope.getExtensionTableAliases(schema, tableName);
    var quotedAlias = quoteName(aliases[ordinalValue - 1]);
    if (property.getOwner() instanceof SQLPropertyExpr) {
      ((SQLPropertyExpr) property.getOwner()).setName(quotedAlias);
    } else {
      property.setOwner(quotedAlias);
    }
    setQueryChanged();
    addAffectedExtensionTables(table.getTableByOrdinalValue(ordinalValue).getName());
  }

  protected String getSchemaOwner(SQLPropertyExpr expr) {
    var schema = SQLStatementUtils.getSchemaOwner(expr);
    if (schema == null) {
      ensureDefaultDatabase();
      schema = defaultDatabase;
    } else {
      schema = unquoteName(schema);
    }
    return schema;
  }

  protected String getTableOwner(SQLPropertyExpr expr) {
    var table = SQLStatementUtils.getTableOwner(expr);
    if (table == null) {
      throw new AmbiguousColumnException(expr.getName(), "table source");
    }
    return unquoteName(table);
  }

  protected String getSchema(final SQLExprTableSource tableSource) {
    var schema = SQLStatementUtils.getSchema(tableSource);
    if (schema == null) {
      ensureDefaultDatabase();
      schema = defaultDatabase;
    }
    return schema;
  }

  protected String getTableName(final SQLExprTableSource tableSource) {
    var tableName = SQLStatementUtils.getTableName(tableSource);
    if (tableName == null) {
      throw new BadSQLException("Bad table source expression.");
    }
    return tableName;
  }

  private void ensureDefaultDatabase() {
    if (defaultDatabase == null) {
      throw new NoDatabaseException();
    }
  }

  protected ColumnMetaData findColumnInScope(
      TableScope scope, String schema, String tableOrAlias, String columnName) {
    ColumnMetaData column = null;
    if (tableOrAlias != null) {
      var table = scope.getTable(schema, tableOrAlias);
      if (table != null) {
        column = table.getColumn(columnName);
      }
    } else {
      column = scope.findColumnInScope(schema, columnName);
    }
    return column;
  }

  protected ColumnMetaData findColumnInScope(
      TableScope scope, SQLExpr columnExpr) {
    ColumnMetaData column = null;
    if (columnExpr instanceof SQLPropertyExpr) {
      var property = (SQLPropertyExpr) columnExpr;
      var schema = getSchemaOwner(property);
      var tableOrAlias = getTableOwner(property);
      var columnName = unquoteName(property.getName());
      column = findColumnInScope(scope, schema, tableOrAlias, columnName);
    } else if (columnExpr instanceof SQLIdentifierExpr) {
      var columnName = unquoteName(((SQLIdentifierExpr) columnExpr).getSimpleName());
      column = findColumnInScope(scope, defaultDatabase, null, columnName);
    }
    return column;
  }

  private void addAffectedExtensionTables(String tableName) {
    if (affectedExtensionTables == null) {
      affectedExtensionTables = new HashSet<>();
    }
    affectedExtensionTables.add(tableName);
  }

  private void checkForAffectedExtensionTables(ColumnMetaData column) {
    var table = column.getTable();
    if (column instanceof TemporaryColumnMetaData) {
      var tmpColumn = (TemporaryColumnMetaData) column;
      table = tmpColumn.getOriginTable();
    }
    if (table.getType() == TableType.EXTENSION) {
      addAffectedExtensionTables(table.getName());
    }
  }
}
