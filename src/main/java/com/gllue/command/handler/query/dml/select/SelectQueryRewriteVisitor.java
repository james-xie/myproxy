package com.gllue.command.handler.query.dml.select;

import static com.gllue.command.handler.query.TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.command.handler.query.TablePartitionHelper.QUOTED_EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.common.util.SQLStatementUtils.getAliasOrTableName;
import static com.gllue.common.util.SQLStatementUtils.getSchema;
import static com.gllue.common.util.SQLStatementUtils.listTableSources;
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
import com.gllue.command.handler.query.NoEncryptKeyException;
import com.gllue.common.exception.BadColumnException;
import com.gllue.common.exception.ColumnExistsException;
import com.gllue.common.exception.NoDatabaseException;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SelectQueryRewriteVisitor extends MySqlASTVisitorAdapter {
  private static final String JOIN_TABLE_ALIAS_PREFIX = "$ext_";

  private final String defaultDatabase;
  private final String encryptKey;
  private final TableScopeFactory tableScopeFactory;

  @Getter private boolean queryChanged = false;
  private TableScope scope;
  private SubQueryTreeNode subQueryTreeRoot = null;
  private Stack<SubQueryTreeNode> subQueryTreeNodeStack;
  private boolean inheritScope = true;
  private boolean shouldVisitProperty = true;
  private int aliasIndex = 0;
  private int selectQueryBlockDepth = 0;
  private String subQueryAlias = null;
  private int subQueryTableSourceDepth = 0;

  public boolean visit(SQLSubqueryTableSource x) {
    inheritScope = false;
    subQueryTableSourceDepth++;
    subQueryAlias = x.getAlias();
    return true;
  }

  public boolean visit(SQLUnionQueryTableSource x) {
    inheritScope = false;
    return true;
  }

  public void endVisit(SQLSubqueryTableSource x) {
    subQueryTableSourceDepth--;
  }

  private void setQueryChanged() {
    queryChanged = true;
  }

  private void newScope(SQLTableSource tableSource) {
    this.scope = tableScopeFactory.newTableScope(scope, inheritScope, tableSource);
    inheritScope = true;
  }

  private void exitScope() {
    assert scope != null;
    this.scope = scope.getParent();
  }

  private boolean collectTableAliases(SQLTableSource tableSource, List<Object> tableAliases) {
    var hasSubQuery = false;
    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      if (source.getAlias() != null) {
        tableAliases.add(source.getAlias());
      } else {
        tableAliases.add(source.getExpr());
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      hasSubQuery = collectTableAliases(source.getLeft(), tableAliases);
      hasSubQuery |= collectTableAliases(source.getRight(), tableAliases);
    } else if (tableSource instanceof SQLUnionQueryTableSource) {
      var source = (SQLUnionQueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
      hasSubQuery = true;
    } else if (tableSource instanceof SQLSubqueryTableSource) {
      var source = (SQLSubqueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
      hasSubQuery = true;
    }

    return hasSubQuery;
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

  private void rewriteSingleTableSelectAllColumnExpr(List<SQLSelectItem> items) {
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

  private SQLTableSource joinExtensionTables(
      String schema, SQLExprTableSource tableSource, PartitionTableMetaData table) {
    if (table.getNumberOfExtensionTables() == 0) {
      return tableSource;
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
    SQLTableSource left = tableSource;
    for (var extTable : extensionTables) {
      var alias = generateTableAlias();
      var quoteAlias = quoteName(alias);
      aliases[i++] = quoteAlias;
      var aliasExpr = new SQLIdentifierExpr(quoteAlias);
      var extTableName = quoteName(extTable.getName());
      var right = new SQLExprTableSource(new SQLIdentifierExpr(extTableName), quoteAlias);
      var condition = newTableJoinCondition(primaryTableAlias, aliasExpr);
      left = new SQLJoinTableSource(left, JoinType.LEFT_OUTER_JOIN, right, condition);
      scope.addTable(schema, alias, extTable);
    }

    var aliasOrTableName = getAliasOrTableName(tableSource);
    scope.addExtensionTableAlias(schema, aliasOrTableName, aliases);

    setQueryChanged();
    return left;
  }

  private SQLTableSource rewriteTableSourceForPartitionTable(SQLTableSource tableSource) {
    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      var schema = getSchema(source);
      if (schema == null) {
        ensureDefaultDatabase();
        schema = defaultDatabase;
      }
      var aliasOrTableName = getAliasOrTableName(source);
      var table = scope.getTable(schema, aliasOrTableName);
      if (table != null && table.getType() == TableType.PARTITION) {
        return joinExtensionTables(schema, source, (PartitionTableMetaData) table);
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      source.setLeft(rewriteTableSourceForPartitionTable(source.getLeft()));
      source.setRight(rewriteTableSourceForPartitionTable(source.getRight()));
    }
    return tableSource;
  }

  private List<String> resolveSubQuerySelectColumns(
      SubQueryTreeNode treeNode, SQLSubqueryTableSource tableSource) {
    if (treeNode == null) {
      return null;
    }

    var queryBlock = tableSource.getSelect().getFirstQueryBlock();
    var selectList = queryBlock.getSelectList();
    tryPromoteColumnsInSubQuery(treeNode, queryBlock.getFrom(), selectList);

    var scope = treeNode.getTableScope();
    var columnNames = new ArrayList<String>();
    var hasAllColumnExpr = false;
    for (var item : selectList) {
      var alias = item.getAlias();
      var expr = item.getExpr();
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var schema = getSchemaOwner(propertyExpr);
        var tableOrAlias = getTableOwner(propertyExpr);
        var name = propertyExpr.getName();
        if (ALL_COLUMN_EXPR.equals(name)) {
          hasAllColumnExpr = true;
        }

        var table = scope.getTable(schema, tableOrAlias);
        if (table != null) {
          var column = table.getColumn(unquoteName(name));
          // Only add encrypted columns to the subQuery tree node.
          if (column.getType() == ColumnType.ENCRYPT) {
            var nameOrAlias = alias == null ? column.getName() : unquoteName(alias);
            treeNode.addColumn(nameOrAlias, column);
          }
        } else if (alias != null) {
          var child = treeNode.getChild(tableOrAlias);
          var column = child != null ? child.lookupColumn(name) : null;
          if (column != null) {
            treeNode.addColumn(unquoteName(alias), column);
          }
        }

        columnNames.add(alias != null ? alias : name);
      } else {
        columnNames.add(alias != null ? alias : expr.toString());
      }
    }

    if (hasAllColumnExpr) {
      return null;
    }

    ensureNoDuplicateColumn(columnNames);
    return columnNames;
  }

  private void ensureNoDuplicateColumn(List<String> columnNames) {
    var columnNameSet = new HashSet<String>();
    for (var column : columnNames) {
      if (columnNameSet.contains(column)) {
        throw new ColumnExistsException(column);
      }
      columnNameSet.add(column);
    }
  }

  private void tryPromoteColumnsInSubQuery(
      SubQueryTreeNode treeNode, SQLTableSource tableSource, List<SQLSelectItem> items) {
    var subQuerySources = listTableSources(tableSource, (x) -> x instanceof SQLSubqueryTableSource);
    if (subQuerySources.isEmpty()) {
      return;
    }

    var aliasMap = new HashMap<String, SQLSubqueryTableSource>();
    var subQueryColumnMap = new HashMap<String, List<String>>();
    for (var source : subQuerySources) {
      var subQuerySource = (SQLSubqueryTableSource) source;
      var alias = subQuerySource.getAlias();
      if (alias == null) {
        throw new NoTableAliasException();
      }
      alias = unquoteName(alias);
      aliasMap.put(alias, subQuerySource);
      subQueryColumnMap.put(
          alias, resolveSubQuerySelectColumns(treeNode.getChild(alias), subQuerySource));
    }

    boolean promoted = false;
    var newItems = new ArrayList<SQLSelectItem>();
    for (var item : items) {
      var expr = item.getExpr();
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var alias = getTableOwner(propertyExpr);
        if (!ALL_COLUMN_EXPR.equals(propertyExpr.getName()) || !aliasMap.containsKey(alias)) {
          newItems.add(item);
          continue;
        }

        var subQueryColumns = subQueryColumnMap.get(alias);
        if (subQueryColumns == null) {
          promoted = false;
          break;
        }

        promoted = true;
        var owner = new SQLIdentifierExpr(alias);
        newItems.addAll(newSQLSelectItems(owner, subQueryColumns));
      } else {
        newItems.add(item);
      }
    }

    if (promoted) {
      items.clear();
      items.addAll(newItems);
      setQueryChanged();
    }
  }

  private SQLExpr rewritePropertyOwnerForEncryptColumn(SQLPropertyExpr property) {
    if (encryptKey == null) {
      throw new NoEncryptKeyException();
    }

    var columnExpr = property.toString();
    var encryptExpr = String.format("AES_DECRYPT(%s, '%s')", columnExpr, encryptKey);
    return new SQLIdentifierExpr(encryptExpr);
  }

  private void rewriteEncryptColumnInSelectItems(List<SQLSelectItem> items) {
    for (var item : items) {
      var expr = item.getExpr();
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var schema = getSchemaOwner(propertyExpr);
        var tableOrAlias = getTableOwner(propertyExpr);
        var name = unquoteName(propertyExpr.getName());
        var table = scope.getTable(schema, tableOrAlias);

        ColumnMetaData column;
        if (table != null) {
          column = table.getColumn(name);
        } else {
          if (ALL_COLUMN_EXPR.equals(name)) {
            var child = subQueryTreeRoot.getChild(tableOrAlias);
            if (child != null && child.anyColumnExists()) {
              throw new BadSQLException(
                  "Some encrypt columns are not promoted to the outer select statement.");
            }
          }

          column = SubQueryTreeNode.lookupColumn(subQueryTreeRoot, tableOrAlias, name);
        }
        if (column != null && column.getType() == ColumnType.ENCRYPT) {
          item.setExpr(rewritePropertyOwnerForEncryptColumn(propertyExpr));
          setQueryChanged();
        }
      }
    }
  }

  public boolean visit(MySqlSelectQueryBlock x) {
    newScope(x.getFrom());

    if (selectQueryBlockDepth == 0) {
      subQueryTreeRoot = new SubQueryTreeNode(null, scope);
      subQueryTreeNodeStack = new Stack<>();
      subQueryTreeNodeStack.push(subQueryTreeRoot);
    } else if (selectQueryBlockDepth == subQueryTableSourceDepth) {
      assert subQueryAlias != null;
      var treeNode = new SubQueryTreeNode(subQueryAlias, scope);
      subQueryTreeNodeStack.peek().addChild(treeNode);
      subQueryTreeNodeStack.push(treeNode);
    }

    selectQueryBlockDepth++;

    shouldVisitProperty = scope.anyTablesInScope();

    var tableAliases = new ArrayList<Object>();
    var hasSubQuery = collectTableAliases(x.getFrom(), tableAliases);
    if (hasSubQuery || shouldVisitProperty) {
      rewriteSelectAllColumnExpr(x.getSelectList(), tableAliases);
    }

    if (shouldVisitProperty) {
      rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
      x.setFrom(rewriteTableSourceForPartitionTable(x.getFrom()));
    }
    return true;
  }

  public void endVisit(MySqlSelectQueryBlock x) {
    if (selectQueryBlockDepth == subQueryTableSourceDepth) {
      subQueryTreeNodeStack.pop();
    }

    selectQueryBlockDepth--;
    if (selectQueryBlockDepth == 0) {
      tryPromoteColumnsInSubQuery(subQueryTreeRoot, x.getFrom(), x.getSelectList());
      rewriteEncryptColumnInSelectItems(x.getSelectList());
    }

    exitScope();
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
    var alias = aliases[ordinalValue - 1];
    if (property.getOwner() instanceof SQLPropertyExpr) {
      ((SQLPropertyExpr) property.getOwner()).setName(alias);
    } else {
      property.setOwner(alias);
    }
    setQueryChanged();
  }

  public boolean visit(SQLPropertyExpr x) {
    if (!shouldVisitProperty) {
      return false;
    }

    var schema = getSchemaOwner(x);
    var tableName = getTableOwner(x);
    var table = scope.getTable(schema, tableName);
    if (table == null) {
      return false;
    }

    var column = unquoteName(x.getName());
    if (EXTENSION_TABLE_ID_COLUMN.equals(column)) {
      return false;
    } else if (!table.hasColumn(column)) {
      throw new BadColumnException(tableName, column);
    }

    if (table.getType() == TableType.PARTITION) {
      rewritePropertyOwnerForPartitionTable(
          x, (PartitionTableMetaData) table, schema, tableName, column);
    }
    return false;
  }

  private String getSchemaOwner(SQLPropertyExpr expr) {
    var schema = SQLStatementUtils.getSchemaOwner(expr);
    if (schema == null) {
      ensureDefaultDatabase();
      schema = defaultDatabase;
    } else {
      schema = unquoteName(schema);
    }
    return schema;
  }

  private String getTableOwner(SQLPropertyExpr expr) {
    var table = SQLStatementUtils.getTableOwner(expr);
    if (table == null) {
      throw new AmbiguousColumnException(expr.getName(), "sql");
    }
    return unquoteName(table);
  }

  private void ensureDefaultDatabase() {
    if (defaultDatabase == null) {
      throw new NoDatabaseException();
    }
  }
}
