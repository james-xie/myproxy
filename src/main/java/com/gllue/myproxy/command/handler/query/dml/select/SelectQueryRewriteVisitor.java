package com.gllue.myproxy.command.handler.query.dml.select;

import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.myproxy.command.handler.query.Decryptor;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import java.util.List;

public class SelectQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private final Decryptor decryptor;
  private int selectQueryBlockDepth = 0;

  public SelectQueryRewriteVisitor(
      String defaultDatabase, TableScopeFactory tableScopeFactory, Decryptor decryptor) {
    super(defaultDatabase, tableScopeFactory);
    this.decryptor = decryptor;
  }

  @Override
  public boolean visit(SQLSelectStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLSelectStatement x) {
    joinExtensionTablesForSelectQueryBlocks();
  }

  public boolean visit(MySqlSelectQueryBlock x) {
    super.visit(x);
    selectQueryBlockDepth++;
    return true;
  }

  public void endVisit(MySqlSelectQueryBlock x) {
    selectQueryBlockDepth--;
    if (selectQueryBlockDepth == 0 && shouldRewriteQuery) {
      rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
      rewriteEncryptColumnInSelectItems(x.getSelectList());
    }

    super.endVisit(x);
  }

  private void rewriteEncryptColumnInSelectItems(List<SQLSelectItem> items) {
    for (var item : items) {
      var expr = item.getExpr();

      ColumnMetaData column = null;
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var schema = getSchemaOwner(propertyExpr);
        var tableOrAlias = getTableOwner(propertyExpr);
        var columnName = unquoteName(propertyExpr.getName());
        var table = scope.getTable(schema, tableOrAlias);
        if (table != null) {
          column = table.getColumn(columnName);
        }
      } else if (expr instanceof SQLIdentifierExpr) {
        var columnName = unquoteName(((SQLIdentifierExpr) expr).getSimpleName());
        column = scope.findColumnInScope(defaultDatabase, columnName);
      }

      if (column != null && column.getType() == ColumnType.ENCRYPT) {
        item.setExpr(decryptColumn(decryptor, expr));
        if (item.getAlias() == null) {
          item.setAlias(quoteName(column.getName()));
        }
        setQueryChanged();
      }
    }
  }
}
