package com.gllue.myproxy.command.handler.query.dml.select;

import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.myproxy.command.handler.query.Decryptor;
import com.gllue.myproxy.command.handler.query.Encryptor;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import java.util.List;

public class SelectQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private final Decryptor decryptor;
  private int selectQueryBlockDepth = 0;
  private SQLUnionQuery unionQuery;

  public SelectQueryRewriteVisitor(
      String defaultDatabase,
      TableScopeFactory tableScopeFactory,
      Encryptor encryptor,
      Decryptor decryptor) {
    super(defaultDatabase, tableScopeFactory, encryptor);
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

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    super.visit(x);
    selectQueryBlockDepth++;
    return true;
  }

  private boolean isUnionQueryRelation(SQLSelectQuery query) {
    return unionQuery != null && unionQuery.getRelations().contains(query);
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    selectQueryBlockDepth--;
    if (shouldRewriteQuery && (selectQueryBlockDepth == 0 || isUnionQueryRelation(x))) {
      rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
      rewriteEncryptColumnInSelectItems(x.getSelectList());
    }

    super.endVisit(x);
  }

  @Override
  public boolean visit(SQLUnionQuery x) {
    unionQuery = x;
    return true;
  }

  private void rewriteEncryptColumnInSelectItems(List<SQLSelectItem> items) {
    for (var item : items) {
      var expr = item.getExpr();

      ColumnMetaData column = findColumnInScope(scope, expr);
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
