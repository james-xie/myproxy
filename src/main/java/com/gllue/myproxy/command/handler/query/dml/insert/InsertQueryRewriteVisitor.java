package com.gllue.myproxy.command.handler.query.dml.insert;

import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.Decryptor;
import com.gllue.myproxy.command.handler.query.Encryptor;
import com.gllue.myproxy.command.handler.query.dml.select.BaseSelectQueryRewriteVisitor;
import com.gllue.myproxy.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.myproxy.common.exception.BadColumnException;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;

public class InsertQueryRewriteVisitor extends BaseSelectQueryRewriteVisitor {
  private static final String VALUES = "values";

  private final String datasource;
  private final MultiDatabasesMetaData databasesMetaData;
  private final IdGenerator idGenerator;
  private final Encryptor encryptor;
  private final Decryptor decryptor;

  @Getter private List<MySqlInsertStatement> newInsertQueries;
  private MySqlInsertStatement insertStatement;

  public InsertQueryRewriteVisitor(
      String defaultDatabase,
      TableScopeFactory tableScopeFactory,
      String datasource,
      MultiDatabasesMetaData databasesMetaData,
      IdGenerator idGenerator,
      Encryptor encryptor,
      Decryptor decryptor) {
    super(defaultDatabase, tableScopeFactory);
    this.datasource = datasource;
    this.databasesMetaData = databasesMetaData;
    this.idGenerator = idGenerator;
    this.encryptor = encryptor;
    this.decryptor = decryptor;
  }

  @Override
  public boolean visit(MySqlInsertStatement x) {
    insertStatement = x;
    if (!x.getValuesList().isEmpty()) {
      return rewriteInsertIntoValues(x);
    }
    return true;
  }

  public void endVisit(MySqlSelectQueryBlock x) {
    var insertQuery = insertStatement.getQuery();
    if (insertQuery != null && insertQuery.getFirstQueryBlock() == x) {
      if (shouldRewriteQuery) {
        rewriteSingleTableSelectAllColumnExpr(x.getSelectList());
        shouldRewriteQuery = false;
      }
      rewriteInsertIntoSelect(insertStatement);
    }

    super.endVisit(x);
  }

  private void ensureColumnExists(SQLPropertyExpr property, String schema, TableMetaData table) {
    var schemaOwner = getSchemaOwner(property);
    var tableOwner = getTableOwner(property);
    var columnName = property.getName();
    if (!schema.equals(schemaOwner)
        || !table.getName().equals(tableOwner)
        || !table.hasColumn(columnName)) {
      throw new BadColumnException(tableOwner, columnName);
    }
  }

  private ColumnMetaData[] getColumns(String schema, TableMetaData table, List<SQLExpr> exprList) {
    var columns = new ArrayList<ColumnMetaData>();
    for (var columnExpr : exprList) {
      if (columnExpr instanceof SQLIdentifierExpr) {
        var identifier = (SQLIdentifierExpr) columnExpr;
        var columnName = unquoteName(identifier.getSimpleName());
        if (!table.hasColumn(columnName)) {
          throw new BadColumnException(table.getName(), columnName);
        }
        columns.add(table.getColumn(columnName));
      } else if (columnExpr instanceof SQLPropertyExpr) {
        var property = (SQLPropertyExpr) columnExpr;
        ensureColumnExists(property, schema, table);
        var columnName = unquoteName(property.getName());
        columns.add(table.getColumn(columnName));
      } else {
        throw new BadSQLException("Bad column expression.");
      }
    }
    return columns.toArray(new ColumnMetaData[0]);
  }

  private boolean isEncryptColumn(String schema, TableMetaData table, SQLExpr columnExpr) {
    if (columnExpr instanceof SQLIdentifierExpr) {
      var identifier = (SQLIdentifierExpr) columnExpr;
      var columnName = unquoteName(identifier.getSimpleName());
      if (!table.hasColumn(columnName)) {
        throw new BadColumnException(table.getName(), columnName);
      }
      return table.getColumn(columnName).getType() == ColumnType.ENCRYPT;
    } else if (columnExpr instanceof SQLPropertyExpr) {
      var property = (SQLPropertyExpr) columnExpr;
      ensureColumnExists(property, schema, table);

      var columnName = unquoteName(property.getName());
      return table.getColumn(columnName).getType() == ColumnType.ENCRYPT;
    }
    return false;
  }

  private int[] findEncryptColumns(ColumnMetaData[] columns) {
    int index = 0;
    var columnIndices = new ArrayList<Integer>();
    for (var column : columns) {
      if (column.getType() == ColumnType.ENCRYPT) {
        columnIndices.add(index);
      }
      index++;
    }
    return Ints.toArray(columnIndices);
  }

  private void rewriteValuesClauseForEncryptColumn(
      int[] encryptColumnIndices, ValuesClause valuesClause) {
    var values = valuesClause.getValues();
    for (var index : encryptColumnIndices) {
      var value = values.get(index);
      if (value instanceof SQLCharExpr) {
        values.set(index, encryptColumn(encryptor, value));
      }
    }
  }

  private SQLExpr getColumnExprInValues(SQLExpr expr) {
    if (expr instanceof SQLMethodInvokeExpr) {
      var invokeExpr = (SQLMethodInvokeExpr) expr;
      if (VALUES.equalsIgnoreCase(invokeExpr.getMethodName())) {
        if (invokeExpr.getArguments().isEmpty()) {
          throw new BadSQLException("Missing 'VALUES' function argument.");
        }

        return invokeExpr.getArguments().get(0);
      }
    }
    return expr;
  }

  private void rewriteDuplicateKeyUpdateForEncryptColumn(
      String schema, TableMetaData table, List<SQLExpr> updateItems) {
    for (var item : updateItems) {
      if (!(item instanceof SQLBinaryOpExpr)) {
        continue;
      }
      var binOp = (SQLBinaryOpExpr) item;
      var left = binOp.getLeft();
      var right = getColumnExprInValues(binOp.getRight());
      var leftIsEncrypt = isEncryptColumn(schema, table, left);
      var rightIsEncrypt = isEncryptColumn(schema, table, right);
      if (leftIsEncrypt == rightIsEncrypt) {
        continue;
      }

      if (leftIsEncrypt) {
        if (right instanceof SQLCharExpr
            || right instanceof SQLIdentifierExpr
            || right instanceof SQLPropertyExpr) {
          binOp.setRight(encryptColumn(encryptor, binOp.getRight()));
        }
      } else {
        binOp.setRight(decryptColumn(decryptor, binOp.getRight()));
      }
      setQueryChanged();
    }
  }

  private boolean columnNotInPrimaryTable(PartitionTableMetaData table, SQLExpr columnExpr) {
    if (columnExpr instanceof SQLIdentifierExpr) {
      var identifier = (SQLIdentifierExpr) columnExpr;
      var columnName = unquoteName(identifier.getSimpleName());
      return !table.getPrimaryTable().hasColumn(columnName);
    } else if (columnExpr instanceof SQLPropertyExpr) {
      var property = (SQLPropertyExpr) columnExpr;
      var columnName = unquoteName(property.getName());
      return !table.getPrimaryTable().hasColumn(columnName);
    }
    return false;
  }

  private void checkDuplicateKeyUpdateForPartitionTable(
      PartitionTableMetaData table, List<SQLExpr> updateItems) {
    for (var item : updateItems) {
      if (!(item instanceof SQLBinaryOpExpr)) {
        continue;
      }
      var binOp = (SQLBinaryOpExpr) item;
      var left = binOp.getLeft();
      var right = getColumnExprInValues(binOp.getRight());
      if (columnNotInPrimaryTable(table, left) || columnNotInPrimaryTable(table, right)) {
        throw new BadSQLException(
            "The affected column in the 'duplicate key update' clause cannot be an extension column.");
      }
    }
  }

  private void rewriteValuesListForEncryptColumn(
      List<ValuesClause> values, ColumnMetaData[] columns) {
    var encryptColumnIndices = findEncryptColumns(columns);
    if (encryptColumnIndices.length == 0) {
      return;
    }

    int i = 0;
    var columnCount = columns.length;
    for (var value : values) {
      i++;
      if (value.getValues().size() != columnCount) {
        throw new ColumnCountNotMatchValueCountException(i);
      }
      rewriteValuesClauseForEncryptColumn(encryptColumnIndices, value);
    }

    setQueryChanged();
  }

  private MySqlInsertStatement newInsertIntoValuesStatement(
      MySqlInsertStatement x,
      String tableName,
      List<String> columns,
      List<SQLExpr> onDuplicateKeyUpdate) {
    var stmt = new MySqlInsertStatement();
    stmt.setIgnore(x.isIgnore());
    stmt.setTableSource(new SQLExprTableSource(quoteName(tableName)));
    for (var column : columns) {
      stmt.addColumn(new SQLIdentifierExpr(quoteName(column)));
    }
    if (onDuplicateKeyUpdate != null) {
      stmt.getDuplicateKeyUpdate().addAll(onDuplicateKeyUpdate);
    }
    return stmt;
  }

  private long[] generateIdsForExtensionTablePrimaryKey(int count) {
    var ids = new long[count];
    for (int i = 0; i < count; i++) {
      ids[i] = idGenerator.nextId();
    }
    return ids;
  }

  private void distributeValues(
      List<ValuesClause> values, List<ValuesClause> newValues, int[] columnIndices, long[] ids) {
    int i = 0;
    for (var value : values) {
      var newValue = new ValuesClause();
      for (int index : columnIndices) {
        newValue.addValue(value.getValues().get(index));
      }
      newValue.addValue(new SQLIntegerExpr(ids[i]));
      newValues.add(newValue);
      i++;
    }
  }

  private void divideInsertQueryIntoMultipleQueries(
      MySqlInsertStatement x, PartitionTableMetaData table, ColumnMetaData[] columns) {
    checkDuplicateKeyUpdateForPartitionTable(table, x.getDuplicateKeyUpdate());

    int index = 0;
    var columnIndicesPerTable = new HashMap<Integer, List<Integer>>();
    var columnNamesPerTable = new HashMap<Integer, List<String>>();
    for (var column : columns) {
      var ordinalValue = table.getOrdinalValueByColumnName(column.getName());
      columnIndicesPerTable.computeIfAbsent(ordinalValue, (k) -> new ArrayList<>()).add(index);
      columnNamesPerTable
          .computeIfAbsent(ordinalValue, (k) -> new ArrayList<>())
          .add(column.getName());
      index++;
    }

    // todo: optimize performance.
    newInsertQueries = new ArrayList<>();
    var values = x.getValuesList();
    var ids = generateIdsForExtensionTablePrimaryKey(values.size());
    for (int i = 0; i < table.getNumberOfTables(); i++) {
      var tableName = table.getTableByOrdinalValue(i).getName();
      List<SQLExpr> duplicateKeyUpdate = null;
      if (i == 0) {
        duplicateKeyUpdate = x.getDuplicateKeyUpdate();
      }

      MySqlInsertStatement newInsert;
      if (columnIndicesPerTable.containsKey(i)) {
        var columnNames = columnNamesPerTable.get(i);
        columnNames.add(EXTENSION_TABLE_ID_COLUMN);
        newInsert = newInsertIntoValuesStatement(x, tableName, columnNames, duplicateKeyUpdate);
        var newValues = newInsert.getValuesList();
        var columnIndices = Ints.toArray(columnIndicesPerTable.get(i));
        distributeValues(values, newValues, columnIndices, ids);
      } else {
        newInsert =
            newInsertIntoValuesStatement(
                x, tableName, List.of(EXTENSION_TABLE_ID_COLUMN), duplicateKeyUpdate);
        var newValues = newInsert.getValuesList();
        for (var id : ids) {
          newValues.add(new ValuesClause(List.of(new SQLIntegerExpr(id))));
        }
      }
      newInsertQueries.add(newInsert);
    }

    setQueryChanged();
  }

  private boolean rewriteInsertIntoValues(MySqlInsertStatement x) {
    var tableSource = x.getTableSource();
    var schema = getSchema(tableSource);
    var tableName = getTableName(tableSource);
    var database = databasesMetaData.getDatabase(datasource, schema);
    var table = database.getTable(tableName);
    if (table == null) {
      return false;
    }

    if (x.getColumns().isEmpty()) {
      throw new BadSQLException("Columns cannot be empty.");
    }

    var columns = getColumns(schema, table, x.getColumns());
    rewriteValuesListForEncryptColumn(x.getValuesList(), columns);

    if (table.getType() == TableType.PARTITION) {
      divideInsertQueryIntoMultipleQueries(x, (PartitionTableMetaData) table, columns);
    }

    rewriteDuplicateKeyUpdateForEncryptColumn(schema, table, x.getDuplicateKeyUpdate());
    return false;
  }

  private void rewriteEncryptColumnInSelectItems(
      List<SQLSelectItem> items, ColumnMetaData[] columns) {
    for (int i = 0; i < items.size(); i++) {
      var item = items.get(i);
      var expr = item.getExpr();

      var isEncryptItem = false;
      ColumnMetaData column = null;
      if (expr instanceof SQLPropertyExpr) {
        var propertyExpr = (SQLPropertyExpr) expr;
        var schema = getSchemaOwner(propertyExpr);
        var tableOrAlias = getTableOwner(propertyExpr);
        var columnName = unquoteName(propertyExpr.getName());
        var table = scope.getTable(schema, tableOrAlias);
        if (table != null) {
          column = table.getColumn(columnName);
          isEncryptItem = column != null && column.getType() == ColumnType.ENCRYPT;
        }
      } else if (expr instanceof SQLIdentifierExpr) {
        var columnName = unquoteName(((SQLIdentifierExpr) expr).getSimpleName());
        column = scope.findColumnInScope(defaultDatabase, columnName);
        isEncryptItem = column != null && column.getType() == ColumnType.ENCRYPT;
      }

      var isEncryptColumn = columns != null && columns[i].getType() == ColumnType.ENCRYPT;

      if (isEncryptItem != isEncryptColumn) {
        if (isEncryptItem) {
          item.setExpr(decryptColumn(decryptor, expr));
        } else {
          item.setExpr(encryptColumn(encryptor, expr));
        }
        setQueryChanged();
      }
    }
  }

  private void rewriteInsertIntoSelect(MySqlInsertStatement x) {
    var tableSource = x.getTableSource();
    var selectQuery = x.getQuery().getFirstQueryBlock();
    var schema = getSchema(tableSource);
    var tableName = getTableName(tableSource);
    var database = databasesMetaData.getDatabase(datasource, schema);
    var table = database.getTable(tableName);
    if (table == null) {
      rewriteEncryptColumnInSelectItems(selectQuery.getSelectList(), null);
      return;
    }

    if (x.getColumns().isEmpty()) {
      throw new BadSQLException("Columns cannot be empty.");
    }

    var columns = getColumns(schema, table, x.getColumns());
    if (table.getType() == TableType.PARTITION) {
      throw new BadSQLException(
          "Cannot execute 'insert into ... select' statement on the partition table.");
    }

    if (selectQuery.getSelectList().size() != columns.length) {
      throw new ColumnCountNotMatchValueCountException(1);
    }

    rewriteEncryptColumnInSelectItems(selectQuery.getSelectList(), columns);
    rewriteDuplicateKeyUpdateForEncryptColumn(schema, table, x.getDuplicateKeyUpdate());
  }
}
