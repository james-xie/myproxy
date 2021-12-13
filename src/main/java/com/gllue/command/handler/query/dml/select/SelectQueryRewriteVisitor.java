package com.gllue.command.handler.query.dml.select;

import static com.gllue.common.util.SQLStatementUtils.unquoteName;
import static com.gllue.constant.ServerConstants.ALL_COLUMN_EXPR;

import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.command.handler.query.NoEncryptKeyException;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import java.util.List;
import java.util.Stack;

public class SelectQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private final String encryptKey;

  private SubQueryTreeNode subQueryTreeRoot = null;
  private Stack<SubQueryTreeNode> subQueryTreeNodeStack;
  private int selectQueryBlockDepth = 0;
  private String subQueryAlias = null;
  private int subQueryTableSourceDepth = 0;

  public SelectQueryRewriteVisitor(
      String defaultDatabase, String encryptKey, TableScopeFactory tableScopeFactory) {
    super(defaultDatabase, tableScopeFactory);
    this.encryptKey = encryptKey;
  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
    super.visit(x);
    subQueryTableSourceDepth++;
    subQueryAlias = x.getAlias();
    return true;
  }

  @Override
  public void endVisit(SQLSubqueryTableSource x) {
    subQueryTableSourceDepth--;
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

    rewriteSelectQuery(x);
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

        ColumnMetaData column;
        if (table != null) {
          column = table.getColumn(name);
        } else {
          // if the select items contains "*" expression, and the subQuery has encrypted
          // column exists, it indicates that some encrypted columns are not promoted to
          // the outer select statement. if we ignore this case, some encrypted columns
          // will not be decrypted.
          if (ALL_COLUMN_EXPR.equals(name)) {
            var child = subQueryTreeRoot.getChild(tableOrAlias);
            if (child != null && child.anyColumnExists()) {
              throw new BadSQLException(
                  "Some encrypted columns are not promoted to the outer select statement.");
            }
          }

          column = SubQueryTreeNode.lookupColumn(subQueryTreeRoot, tableOrAlias, name);
        }
        if (column != null && column.getType() == ColumnType.ENCRYPT) {
          if (encryptKey == null) {
            throw new NoEncryptKeyException();
          }

          item.setExpr(rewritePropertyOwnerForEncryptColumn(encryptKey, propertyExpr));
          setQueryChanged();
        }
      }
    }
  }
}
