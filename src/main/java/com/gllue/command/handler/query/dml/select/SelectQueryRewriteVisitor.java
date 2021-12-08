package com.gllue.command.handler.query.dml.select;

import static com.gllue.command.handler.query.TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.command.handler.query.TablePartitionHelper.QUOTED_EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.common.util.SQLStatementUtils.getAliasOrTableName;
import static com.gllue.common.util.SQLStatementUtils.getSchema;
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
import com.gllue.command.handler.query.NoEncryptKeyException;
import com.gllue.command.handler.query.TablePartitionHelper;
import com.gllue.common.exception.BadColumnException;
import com.gllue.common.exception.NoDatabaseException;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private boolean inheritScope = true;
  private boolean shouldVisitProperty = true;
  private int aliasIndex = 0;

  public boolean visit(SQLSubqueryTableSource x) {
    inheritScope = false;
    return true;
  }

  public boolean visit(SQLUnionQueryTableSource x) {
    inheritScope = false;
    return true;
  }

  public void endVisit(SQLSubqueryTableSource x) {}

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

  private void collectTableAliases(SQLTableSource tableSource, List<Object> tableAliases) {
    if (tableSource instanceof SQLExprTableSource) {
      var source = (SQLExprTableSource) tableSource;
      if (source.getAlias() != null) {
        tableAliases.add(source.getAlias());
      } else {
        tableAliases.add(source.getExpr());
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      collectTableAliases(source.getLeft(), tableAliases);
      collectTableAliases(source.getRight(), tableAliases);
    } else if (tableSource instanceof SQLUnionQueryTableSource) {
      var source = (SQLUnionQueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
    } else if (tableSource instanceof SQLSubqueryTableSource) {
      var source = (SQLSubqueryTableSource) tableSource;
      if (source.getAlias() == null) {
        throw new NoTableAliasException();
      }
      tableAliases.add(source.getAlias());
    }
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
      SQLExprTableSource tableSource, PartitionTableMetaData table) {
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
      var alias = quoteName(generateTableAlias());
      aliases[i++] = alias;
      var aliasExpr = new SQLIdentifierExpr(alias);
      var extTableName = quoteName(extTable.getName());
      var right = new SQLExprTableSource(new SQLIdentifierExpr(extTableName), alias);
      var condition = newTableJoinCondition(primaryTableAlias, aliasExpr);
      left = new SQLJoinTableSource(left, JoinType.LEFT_OUTER_JOIN, right, condition);
    }

    var schema = getSchema(tableSource);
    if (schema == null) {
      ensureDefaultDatabase();
      schema = defaultDatabase;
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
        return joinExtensionTables(source, (PartitionTableMetaData) table);
      }
    } else if (tableSource instanceof SQLJoinTableSource) {
      var source = (SQLJoinTableSource) tableSource;
      source.setLeft(rewriteTableSourceForPartitionTable(source.getLeft()));
      source.setRight(rewriteTableSourceForPartitionTable(source.getRight()));
    }
    return tableSource;
  }

  public boolean visit(MySqlSelectQueryBlock x) {
    newScope(x.getFrom());
    shouldVisitProperty = scope.anyTablesInScope();
    if (shouldVisitProperty) {
      var tableAliases = new ArrayList<Object>();
      collectTableAliases(x.getFrom(), tableAliases);
      rewriteSelectAllColumnExpr(x.getSelectList(), tableAliases);
      rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
      x.setFrom(rewriteTableSourceForPartitionTable(x.getFrom()));
    }
    return true;
  }

  public void endVisit(MySqlSelectQueryBlock x) {
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

  private void rewritePropertyOwnerForEncryptColumn(SQLPropertyExpr property) {
    if (encryptKey == null) {
      throw new NoEncryptKeyException();
    }

    var columnExpr = property.toString();
    var encryptExpr = String.format("AES_DECRYPT(%s, '%s')", columnExpr, encryptKey);
    property.setName(encryptExpr);
    property.setOwner((SQLExpr) null);
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
    if(EXTENSION_TABLE_ID_COLUMN.equals(column)) {
      return false;
    } else if (!table.hasColumn(column)) {
      throw new BadColumnException(tableName, column);
    }

    if (table.getType() == TableType.PARTITION) {
      rewritePropertyOwnerForPartitionTable(
          x, (PartitionTableMetaData) table, schema, tableName, column);
    }
    if (table.getColumn(column).getType() == ColumnType.ENCRYPT) {
      rewritePropertyOwnerForEncryptColumn(x);
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
