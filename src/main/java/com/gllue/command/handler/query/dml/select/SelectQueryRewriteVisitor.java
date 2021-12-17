package com.gllue.command.handler.query.dml.select;

import static com.gllue.common.util.SQLStatementUtils.quoteName;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.command.handler.query.NoEncryptKeyException;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import java.util.List;

public class SelectQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private final String encryptKey;

  private int selectQueryBlockDepth = 0;

  public SelectQueryRewriteVisitor(
      String defaultDatabase, TableScopeFactory tableScopeFactory, String encryptKey) {
    super(defaultDatabase, tableScopeFactory);
    this.encryptKey = encryptKey;
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
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var schema = getSchemaOwner(propertyExpr);
        var tableOrAlias = getTableOwner(propertyExpr);
        var name = unquoteName(propertyExpr.getName());
        var table = scope.getTable(schema, tableOrAlias);

        ColumnMetaData column = null;
        if (table != null) {
          column = table.getColumn(name);
        }
        if (column != null && column.getType() == ColumnType.ENCRYPT) {
          if (encryptKey == null) {
            throw new NoEncryptKeyException();
          }
          
          item.setExpr(decryptColumn(encryptKey, propertyExpr));
          if (item.getAlias() == null) {
            item.setAlias(quoteName(name));
          }
          setQueryChanged();
        }
      }
    }
  }
}
