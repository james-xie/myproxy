package com.gllue.command.handler.query.dml.update;

import static com.gllue.command.handler.query.TablePartitionHelper.QUOTED_EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.command.handler.query.TablePartitionHelper.constructSubQueryToFilter;
import static com.gllue.command.handler.query.TablePartitionHelper.newTableJoinCondition;
import static com.gllue.common.util.SQLStatementUtils.anySubQueryExists;
import static com.gllue.common.util.SQLStatementUtils.listTableSources;
import static com.gllue.common.util.SQLStatementUtils.quoteName;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.command.handler.query.NoEncryptKeyException;
import com.gllue.command.handler.query.dml.select.BaseSelectQueryRewriteVisitor;
import com.gllue.command.handler.query.dml.select.SubQueryTreeNode;
import com.gllue.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class UpdateQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private final String encryptKey;

  private boolean shouldTransform = false;
  private SQLTableSource originTableSource;

  private SubQueryTreeNode subQueryTreeRoot = null;
  private Stack<SubQueryTreeNode> subQueryTreeNodeStack;
  private String subQueryAlias = null;

  public UpdateQueryRewriteVisitor(
      String defaultDatabase, TableScopeFactory tableScopeFactory, String encryptKey) {
    super(defaultDatabase, tableScopeFactory);
    this.encryptKey = encryptKey;
  }

  @Override
  public boolean visit(MySqlUpdateStatement x) {
    var tableSource = x.getTableSource();
    newScope(tableSource);
    x.setTableSource(rewriteTableSourceForPartitionTable(tableSource));

    if (joinedExtensionTables) {
      shouldVisitProperty = true;
      originTableSource = tableSource;
    }
    if (scope.anyTablesInScope() || anySubQueryExists(tableSource)) {
      shouldTransform = true;
      originTableSource = tableSource;
    }

    subQueryTreeRoot = new SubQueryTreeNode(null, scope);
    subQueryTreeNodeStack = new Stack<>();
    subQueryTreeNodeStack.push(subQueryTreeRoot);
    return true;
  }

  @Override
  public void endVisit(MySqlUpdateStatement x) {
    if (shouldTransform) {
      if (originTableSource instanceof SQLJoinTableSource) {
        rewriteMultiTableUpdate(x);
      } else {
        assert originTableSource instanceof SQLExprTableSource;
        rewriteSingleTableUpdate(x);
      }
    }
  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
    var res = super.visit(x);
    subQueryAlias = unquoteName(x.getAlias());
    return res;
  }

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    var res = super.visit(x);

    assert subQueryAlias != null;
    var treeNode = new SubQueryTreeNode(subQueryAlias, scope);
    subQueryTreeNodeStack.peek().addChild(treeNode);
    subQueryTreeNodeStack.push(treeNode);
    return res;
  }

  public void endVisit(MySqlSelectQueryBlock x) {
    subQueryTreeNodeStack.pop();
    super.endVisit(x);
  }

  private void transformSingleTableUpdateToJoinSubQueryUpdate(
      MySqlUpdateStatement x, SQLTableSource tableSource) {
    SQLExpr tableSourceAlias;
    if (originTableSource.getAlias() != null) {
      tableSourceAlias = new SQLIdentifierExpr(originTableSource.getAlias());
    } else {
      tableSourceAlias = ((SQLExprTableSource) originTableSource).getExpr();
    }

    var orderBy = x.getOrderBy();
    var limit = x.getLimit();
    x.setOrderBy(null);
    x.setLimit(null);
    var where = x.getWhere();
    x.setWhere(null);
    var filterSubQuery =
        constructSubQueryToFilter(tableSource, tableSourceAlias, where, orderBy, limit);

    var condition =
        newTableJoinCondition(tableSourceAlias, new SQLIdentifierExpr(filterSubQuery.getAlias()));
    x.setTableSource(new SQLJoinTableSource(tableSource, JoinType.INNER_JOIN, filterSubQuery, condition));
  }

  private boolean isEncryptColumn(ColumnMetaData column) {
    if (column == null) {
      return false;
    }
    return column.getType() == ColumnType.ENCRYPT;
  }

  private void ensureEncryptKeyExists() {
    if (encryptKey == null) {
      throw new NoEncryptKeyException();
    }
  }

  private boolean onlyUpdatedExtensionTables(
      PartitionTableMetaData table, Set<String> updateTables) {
    if (updateTables.contains(table.getName())) {
      return false;
    }
    for (var extTable : table.getExtensionTables()) {
      if (updateTables.contains(extTable.getName())) {
        return true;
      }
    }
    return false;
  }

  private SQLUpdateSetItem newUpdatePrimaryTableItem(String schema, String tableOrAlias) {
    var item = new SQLUpdateSetItem();
    SQLExpr columnExpr;
    var columnName = QUOTED_EXTENSION_TABLE_ID_COLUMN;
    if (schema == null) {
      columnExpr = new SQLPropertyExpr(quoteName(tableOrAlias), columnName);
    } else {
      columnExpr =
          new SQLPropertyExpr(
              new SQLPropertyExpr(quoteName(schema), quoteName(tableOrAlias)), columnName);
    }

    item.setColumn(columnExpr);
    item.setValue(columnExpr);
    return item;
  }

  private void ensureAlwaysUpdatePrimaryTable(
      List<SQLUpdateSetItem> items, List<ColumnMetaData> updateColumns) {
    var updateTables =
        updateColumns.stream().map(x -> x.getTable().getName()).collect(Collectors.toSet());

    var tableSources = listTableSources(originTableSource, x -> x instanceof SQLExprTableSource);
    for (var item : tableSources) {
      var tableSource = (SQLExprTableSource) item;
      var schema = getSchema(tableSource);
      var tableName = getTableName(tableSource);
      var table = scope.getTable(schema, tableName);
      if (table == null || table.getType() != TableType.PARTITION) {
        continue;
      }
      if (onlyUpdatedExtensionTables((PartitionTableMetaData) table, updateTables)) {
        var tableOrAlias = tableSource.getAlias() == null ? tableName : tableSource.getAlias();
        items.add(newUpdatePrimaryTableItem(schema, tableOrAlias));
      }
    }
  }

  private boolean processEncryptColumnInUpdateItems(
      List<SQLUpdateSetItem> items, List<ColumnMetaData> updateColumns) {
    var hasEncryptColumn = false;
    for (var item : items) {
      var columnExpr = item.getColumn();
      var valueExpr = item.getValue();
      var column = findColumnInScopeOrSubQuery(scope, subQueryTreeRoot, columnExpr);
      var value = findColumnInScopeOrSubQuery(scope, subQueryTreeRoot, valueExpr);
      if (isEncryptColumn(column) && !isEncryptColumn(value)) {
        ensureEncryptKeyExists();
        item.setValue(encryptColumn(encryptKey, valueExpr));
        hasEncryptColumn = true;
      } else if (!isEncryptColumn(column) && isEncryptColumn(value)) {
        ensureEncryptKeyExists();
        item.setValue(decryptColumn(encryptKey, valueExpr));
        hasEncryptColumn = true;
      }
      if (column != null) {
        updateColumns.add(column);
      }
    }
    return hasEncryptColumn;
  }

  private void rewriteSingleTableUpdate(MySqlUpdateStatement x) {
    var updateColumns = new ArrayList<ColumnMetaData>();
    var hasEncryptColumn = processEncryptColumnInUpdateItems(x.getItems(), updateColumns);
    if (!joinedExtensionTables && !hasEncryptColumn) {
      return;
    }

    if (joinedExtensionTables) {
      ensureAlwaysUpdatePrimaryTable(x.getItems(), updateColumns);
    }

    if (x.getOrderBy() != null || x.getLimit() != null) {
      transformSingleTableUpdateToJoinSubQueryUpdate(x, x.getTableSource());
    }

    setQueryChanged();
  }

  private void rewriteMultiTableUpdate(MySqlUpdateStatement x) {
    if (x.getFrom() != null || x.getLimit() != null) {
      throw new BadSQLException("Multiple table update does not support orderBy/limit clause.");
    }
    tryPromoteColumnsInSubQuery(subQueryTreeRoot, x.getTableSource(), List.of());
    rewriteSingleTableUpdate(x);
  }
}
