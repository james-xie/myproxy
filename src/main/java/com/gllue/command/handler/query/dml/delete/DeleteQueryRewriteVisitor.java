package com.gllue.command.handler.query.dml.delete;

import static com.gllue.command.handler.query.TablePartitionHelper.QUOTED_EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.command.handler.query.TablePartitionHelper.newTableJoinCondition;
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
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.command.handler.query.dml.select.BaseSelectQueryRewriteVisitor;
import com.gllue.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import java.util.ArrayList;

public class DeleteQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private static final String SUB_QUERY_ALIAS = "$_sub_query";
  private static final String QUOTED_SUB_QUERY_ALIAS = String.format("`%s`", SUB_QUERY_ALIAS);

  private boolean shouldTransform = false;
  private SQLTableSource originTableSource;

  public DeleteQueryRewriteVisitor(String defaultDatabase, TableScopeFactory tableScopeFactory) {
    super(defaultDatabase, tableScopeFactory);
  }

  @Override
  public boolean visit(MySqlDeleteStatement x) {
    SQLTableSource tableSource;
    if (x.getUsing() != null) {
      tableSource = x.getUsing();
      newScope(tableSource);
      x.setUsing(rewriteTableSourceForPartitionTable(tableSource));
    } else if (x.getFrom() != null) {
      tableSource = x.getFrom();
      newScope(tableSource);
      x.setFrom(rewriteTableSourceForPartitionTable(tableSource));
    } else {
      tableSource = x.getTableSource();
      newScope(tableSource);
      x.setTableSource(rewriteTableSourceForPartitionTable(tableSource));
    }

    if (isQueryChanged()) {
      shouldTransform = true;
      shouldVisitProperty = true;
      originTableSource = tableSource;
    }
    return true;
  }

  @Override
  public void endVisit(MySqlDeleteStatement x) {
    if (shouldTransform) {
      if (x.getUsing() != null || x.getFrom() != null) {
        rewriteMultiTableDelete(x);
      } else {
        rewriteSingleTableDelete(x);
      }
    }
  }

  private SQLTableSource generateCommaJoinedTableSource(SQLTableSource tableSource) {
    var tableAliases = new ArrayList<Object>();
    collectTableAliases(tableSource, tableAliases);

    SQLTableSource newTableSource = null;
    for (var tableAlias : tableAliases) {
      SQLTableSource source;
      if (tableAlias instanceof SQLExpr) {
        source = new SQLExprTableSource((SQLExpr) tableAlias);
      } else {
        source = new SQLExprTableSource((String) tableAlias);
      }

      if (newTableSource == null) {
        newTableSource = source;
      } else {
        newTableSource = new SQLJoinTableSource(newTableSource, JoinType.COMMA, source, null);
      }
    }
    return newTableSource;
  }

  private void transformSingleTableDeleteToMultiTableDelete(
      MySqlDeleteStatement x, SQLTableSource tableSource) {
    x.setFrom(tableSource);
    x.setTableSource(generateCommaJoinedTableSource(tableSource));
  }

  private SQLTableSource constructSubQueryToFilterDelete(
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
    return new SQLSubqueryTableSource(new SQLSelect(select));
  }

  private void transformSingleTableDeleteToMultiTableDelete(
      MySqlDeleteStatement x, SQLOrderBy orderBy, SQLLimit limit, SQLTableSource tableSource) {
    if (!(originTableSource instanceof SQLExprTableSource)) {
      throw new BadSQLException("Single table delete does not support join clause.");
    }

    SQLExpr tableSourceAlias;
    if (originTableSource.getAlias() != null) {
      tableSourceAlias = new SQLIdentifierExpr(originTableSource.getAlias());
    } else {
      tableSourceAlias = ((SQLExprTableSource) originTableSource).getExpr();
    }

    x.setTableSource(generateCommaJoinedTableSource(tableSource));
    var where = x.getWhere();
    x.setWhere(null);
    var filterSubQuery =
        constructSubQueryToFilterDelete(tableSource, tableSourceAlias, where, orderBy, limit);
    filterSubQuery.setAlias(QUOTED_SUB_QUERY_ALIAS);

    var condition =
        newTableJoinCondition(tableSourceAlias, new SQLIdentifierExpr(QUOTED_SUB_QUERY_ALIAS));
    x.setFrom(new SQLJoinTableSource(tableSource, JoinType.INNER_JOIN, filterSubQuery, condition));
  }

  private void rewriteSingleTableDelete(MySqlDeleteStatement x) {
    var orderBy = x.getOrderBy();
    var limit = x.getLimit();
    if (orderBy == null && limit == null) {
      transformSingleTableDeleteToMultiTableDelete(x, x.getTableSource());
    } else {
      x.setOrderBy(null);
      x.setLimit(null);
      transformSingleTableDeleteToMultiTableDelete(x, orderBy, limit, x.getTableSource());
    }
  }

  private String[] getExtensionTableAliases(Object tableAlias) {
    String[] extensionTableAliases = null;
    if (tableAlias instanceof SQLPropertyExpr) {
      var property = (SQLPropertyExpr) tableAlias;
      var schema = unquoteName(((SQLIdentifierExpr) (property.getOwner())).getSimpleName());
      var tableName = unquoteName(property.getName());
      extensionTableAliases = scope.getExtensionTableAliases(schema, tableName);
    } else if (tableAlias instanceof SQLIdentifierExpr) {
      var tableName = unquoteName(((SQLIdentifierExpr) tableAlias).getSimpleName());
      extensionTableAliases = scope.getExtensionTableAliases(defaultDatabase, tableName);
    } else if (tableAlias instanceof String) {
      var alias = unquoteName((String) tableAlias);
      extensionTableAliases = scope.getExtensionTableAliases(defaultDatabase, alias);
    }
    return extensionTableAliases;
  }

  private void rewriteMultiTableDelete(MySqlDeleteStatement x) {
    if (x.getOrderBy() != null || x.getLimit() != null) {
      throw new BadSQLException("Multiple table delete does not support orderBy/limit clause.");
    }

    var tableSource = x.getTableSource();
    var tableAliases = new ArrayList<Object>();
    collectTableAliases(tableSource, tableAliases);

    for (var alias : tableAliases) {
      String[] extensionTableAliases = getExtensionTableAliases(alias);
      if (extensionTableAliases == null) {
        continue;
      }

      for (var extTableAlias : extensionTableAliases) {
        var source = new SQLExprTableSource(quoteName(extTableAlias));
        tableSource = new SQLJoinTableSource(tableSource, JoinType.COMMA, source, null);
      }
    }

    x.setTableSource(tableSource);
  }
}
