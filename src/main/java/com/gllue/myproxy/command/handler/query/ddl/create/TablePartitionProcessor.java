package com.gllue.myproxy.command.handler.query.ddl.create;

import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.generateExtensionTableName;
import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.generatePrimaryTableName;
import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.newExtensionTableIdColumn;
import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.newExtensionTablePrimaryKey;
import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.newKeyForExtensionTableIdColumn;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getForeignKeyReferencingColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getIndexColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getPrimaryKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getUniqueKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newCreateTableStatement;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.SQLIndex;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyConstraint;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLUniqueConstraint;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.command.handler.query.BadCommentAttributeException;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A processor that handles table partition.
 *
 * <p>The table partition is to divide a large wide table into multiple parts, each part has a
 * number of columns. Every partition table is consists of a primary table and some extension
 * tables. The extension table is used to manage extension columns, and other columns are managed by
 * the primary table.
 *
 * <p>To create a partition table, just set the attribute "PARTITION_TABLE: true" in the comments.
 * The extension columns are declared by the 'EXTENSION_COLUMNS' attribute in the sql comments. If
 * the column name is declared as extension column, it will be added to an extension table.
 *
 * <pre>
 *   For example:
 *    # { PARTITION_TABLE: true, EXTENSION_COLUMNS: ["column1", "column2", ...] }
 *    CREATE TABLE ... {
 *      ...
 *      `column1` int(11) not null,
 *      `column2` int(11) not null,
 *      ...
 *    }
 * </pre>
 */
@RequiredArgsConstructor
class TablePartitionProcessor {

  private static final int PRIMARY_TABLE_ORDINAL_VALUE = 0;

  private final Configurations configurations;
  private final Map<SQLCommentAttributeKey, Object> attributes;

  @Getter private boolean prepared = false;
  private Set<String> primaryColumns;
  private Set<String> extensionColumns;
  private Map<String, Integer> columnOrdinalValueMap;

  private void validateColumns(String[] extensionColumns, List<String> columnNames) {
    var columnNameSet = new HashSet<>(columnNames);
    if (columnNameSet.size() != columnNames.size()) {
      throw new BadSQLException(
          "Found %s duplicate columns.", columnNames.size() - columnNameSet.size());
    }

    for (var columnName : extensionColumns) {
      if (!columnNameSet.contains(columnName)) {
        throw new BadCommentAttributeException(
            SQLCommentAttributeKey.EXTENSION_COLUMNS, columnName);
      }
    }
  }

  private void validateStatement(MySqlCreateTableStatement stmt) {
    if (stmt.getTablePartitionBy() != null || stmt.getPartitioning() != null) {
      throw new BadSQLException(
          "Add extension column to a table with 'partition by' clause is not allowed.");
    }

    if (stmt.getType() != null) {
      throw new BadSQLException("Add extension column on a temporary table is not allowed.");
    }
  }

  private Map<String, Integer> buildColumnOrdinalValueMap(
      List<String> columnNames, int maxColumnsInExtensionTable) {

    int extensionColumnOrdinalValue = 1;
    int extensionColumnCount = 0;
    var ordinalValueMap = new HashMap<String, Integer>();
    for (var columnName : columnNames) {
      if (primaryColumns.contains(columnName)) {
        ordinalValueMap.put(columnName, PRIMARY_TABLE_ORDINAL_VALUE);
      } else {
        ordinalValueMap.put(columnName, extensionColumnOrdinalValue);
        extensionColumnCount++;
        if (extensionColumnCount >= maxColumnsInExtensionTable) {
          extensionColumnCount = 0;
          extensionColumnOrdinalValue++;
        }
      }
    }
    return ordinalValueMap;
  }

  boolean prepare(MySqlCreateTableStatement stmt) {
    var isPartitionTable =
        (boolean) attributes.getOrDefault(SQLCommentAttributeKey.PARTITION_TABLE, false);
    if (!isPartitionTable) {
      return false;
    }

    var extensionColumns = (String[]) attributes.get(SQLCommentAttributeKey.EXTENSION_COLUMNS);
    if (extensionColumns == null) {
      extensionColumns = new String[0];
    }

    var columnNames =
        stmt.getColumnDefinitions().stream()
            .map(c -> unquoteName(c.getColumnName()))
            .collect(Collectors.toList());

    validateColumns(extensionColumns, columnNames);
    validateStatement(stmt);

    this.extensionColumns = new HashSet<>(Arrays.asList(extensionColumns));
    this.primaryColumns =
        columnNames.stream()
            .filter(c -> !this.extensionColumns.contains(c))
            .collect(Collectors.toSet());
    if (this.primaryColumns.isEmpty()) {
      throw new BadSQLException("Primary table has no column definition.");
    }

    int maxColumnsPerTable =
        configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE);
    double columnsAllocationWatermark =
        configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK);

    this.columnOrdinalValueMap =
        buildColumnOrdinalValueMap(
            columnNames, (int) (maxColumnsPerTable * columnsAllocationWatermark));

    prepared = true;
    return true;
  }

  static class TableDefinition {

    int ordinalValue;
    SQLTableElement primaryKey;
    List<SQLTableElement> columnDefs;
    List<SQLTableElement> indices;
    List<SQLTableElement> constraints;

    void addColumnDefinition(SQLTableElement columnDef) {
      addColumnDefinition(-1, columnDef);
    }

    void addColumnDefinition(int index, SQLTableElement columnDef) {
      if (this.columnDefs == null) {
        this.columnDefs = new ArrayList<>();
      }
      if (index >= 0) {
        this.columnDefs.add(index, columnDef);
      } else {
        this.columnDefs.add(columnDef);
      }
    }

    void addIndex(SQLTableElement index) {
      if (this.indices == null) {
        this.indices = new ArrayList<>();
      }
      this.indices.add(index);
    }

    void addConstraint(SQLTableElement constraint) {
      if (this.constraints == null) {
        this.constraints = new ArrayList<>();
      }
      this.constraints.add(constraint);
    }
  }

  enum AddOperation {
    PRIMARY_KEY,
    COLUMN_DEFS,
    INDEXES,
    CONSTRAINTS
  }

  static class TableDefinitionGroup {

    List<TableDefinition> definitions = new ArrayList<>();

    private void ensureDefinitionCapacity(int ordinalValue) {
      if (ordinalValue >= definitions.size()) {
        for (int i = definitions.size(); i <= ordinalValue; i++) {
          var definition = new TableDefinition();
          definition.ordinalValue = i;
          definitions.add(definition);
        }
      }
    }

    TableDefinition getPrimaryTableDefinition() {
      if (definitions.size() <= PRIMARY_TABLE_ORDINAL_VALUE) {
        return null;
      }
      return definitions.get(PRIMARY_TABLE_ORDINAL_VALUE);
    }

    List<TableDefinition> getExtensionTableDefinitions() {
      var extDefinitions = new ArrayList<TableDefinition>();
      for (int i = 0; i < definitions.size(); i++) {
        if (i == PRIMARY_TABLE_ORDINAL_VALUE) {
          continue;
        }
        extDefinitions.add(definitions.get(i));
      }
      return extDefinitions;
    }

    void addTableElement(SQLTableElement element, int ordinalValue, AddOperation operation) {
      ensureDefinitionCapacity(ordinalValue);
      var definition = definitions.get(ordinalValue);
      switch (operation) {
        case PRIMARY_KEY:
          assert definition.primaryKey == null;
          definition.primaryKey = element;
          break;
        case COLUMN_DEFS:
          definition.addColumnDefinition(element);
          break;
        case INDEXES:
          definition.addIndex(element);
          break;
        case CONSTRAINTS:
          definition.addConstraint(element);
          break;
      }
    }
  }

  private boolean columnsAcrossPrimaryTableAndExtTable(String[] columns) {
    if (columns.length <= 1) {
      return false;
    }

    boolean columnInPrimaryTable = false;
    boolean columnInExtensionTable = false;
    for (var column : columns) {
      if (!columnInExtensionTable && extensionColumns.contains(column)) {
        columnInExtensionTable = true;
      }
      if (!columnInPrimaryTable && primaryColumns.contains(column)) {
        columnInPrimaryTable = true;
      }
    }
    return columnInPrimaryTable && columnInExtensionTable;
  }

  /**
   * Partition the table elements into multiple parts.
   *
   * @param elements of the 'create table' statement.
   * @return partition result.
   */
  TableDefinitionGroup partitionTableElements(List<SQLTableElement> elements) {
    TableDefinitionGroup tableGroup = new TableDefinitionGroup();
    for (var element : elements) {
      if (element instanceof SQLColumnDefinition) {
        var columnDef = (SQLColumnDefinition) element;
        var columnName = unquoteName(columnDef.getColumnName());
        var ordinalValue = columnOrdinalValueMap.get(columnName);
        tableGroup.addTableElement(element, ordinalValue, AddOperation.COLUMN_DEFS);
      } else if (element instanceof SQLPrimaryKey) {
        var columns = getPrimaryKeyColumns((SQLPrimaryKey) element);
        for (var column : columns) {
          if (extensionColumns.contains(column)) {
            throw new BadSQLException("Columns in primary key cannot be an extension column.");
          }
        }

        tableGroup.addTableElement(element, PRIMARY_TABLE_ORDINAL_VALUE, AddOperation.PRIMARY_KEY);
      } else if (element instanceof SQLUniqueConstraint) {
        var columns = getUniqueKeyColumns((SQLUniqueConstraint) element);
        if (columnsAcrossPrimaryTableAndExtTable(columns)) {
          throw new BadSQLException(
              "Columns in unique key across primary table and extension table is not allowed.");
        }
        if (columns.length > 1 && extensionColumns.contains(columns[0])) {
          throw new BadSQLException("Union index in extension table is not allowed.");
        }

        var ordinalValue = columnOrdinalValueMap.get(columns[0]);
        tableGroup.addTableElement(element, ordinalValue, AddOperation.CONSTRAINTS);
      } else if (element instanceof SQLIndex) {
        var columns = getIndexColumns((SQLIndex) element);
        if (columnsAcrossPrimaryTableAndExtTable(columns)) {
          throw new BadSQLException(
              "Columns in table index across primary table and extension table is not allowed.");
        }
        if (columns.length > 1 && extensionColumns.contains(columns[0])) {
          throw new BadSQLException("Union index in extension table is not allowed.");
        }

        var ordinalValue = columnOrdinalValueMap.get(columns[0]);
        tableGroup.addTableElement(element, ordinalValue, AddOperation.INDEXES);
      } else if (element instanceof SQLForeignKeyConstraint) {
        var columns = getForeignKeyReferencingColumns((SQLForeignKeyConstraint) element);
        if (columnsAcrossPrimaryTableAndExtTable(columns)) {
          throw new BadSQLException(
              "Columns in foreign key across primary table and extension table is not allowed.");
        }
        if (columns.length > 1 && extensionColumns.contains(columns[0])) {
          throw new BadSQLException("Union foreign key in extension table is not allowed.");
        }

        var ordinalValue = columnOrdinalValueMap.get(columns[0]);
        tableGroup.addTableElement(element, ordinalValue, AddOperation.CONSTRAINTS);
      }
    }
    return tableGroup;
  }

  private MySqlCreateTableStatement buildPrimaryTableStatement(
      String tableName, TableDefinition definition, List<SQLAssignItem> tableOptions) {
    if (definition.primaryKey == null) {
      throw new BadSQLException("Missing primary key in primary table.");
    }
    if (definition.columnDefs == null || definition.columnDefs.isEmpty()) {
      throw new BadSQLException("No column definition in primary table.");
    }

    definition.addColumnDefinition(0, newExtensionTableIdColumn());
    definition.addIndex(newKeyForExtensionTableIdColumn());

    return newCreateTableStatement(
        tableName,
        definition.columnDefs,
        definition.primaryKey,
        definition.indices,
        definition.constraints,
        tableOptions,
        false);
  }

  private MySqlCreateTableStatement buildExtensionTableStatement(
      String tableName, TableDefinition definition, List<SQLAssignItem> tableOptions) {
    assert definition.primaryKey == null;
    if (definition.columnDefs == null || definition.columnDefs.isEmpty()) {
      throw new BadSQLException("No column definition in extension table.");
    }

    definition.addColumnDefinition(0, newExtensionTableIdColumn());
    definition.primaryKey = newExtensionTablePrimaryKey();
    return newCreateTableStatement(
        tableName,
        definition.columnDefs,
        definition.primaryKey,
        definition.indices,
        definition.constraints,
        tableOptions,
        false);
  }

  List<MySqlCreateTableStatement> processStatement(List<MySqlCreateTableStatement> stmtList) {
    if (!prepared) {
      return stmtList;
    }

    List<MySqlCreateTableStatement> partitionedStmtList = new ArrayList<>();
    for (var stmt : stmtList) {
      var tableName = unquoteName(stmt.getTableName());
      var tableGroup = partitionTableElements(stmt.getTableElementList());
      partitionedStmtList.add(
          buildPrimaryTableStatement(
              generatePrimaryTableName(tableName),
              tableGroup.getPrimaryTableDefinition(),
              stmt.getTableOptions()));
      for (var definition : tableGroup.getExtensionTableDefinitions()) {
        partitionedStmtList.add(
            buildExtensionTableStatement(
                generateExtensionTableName(tableName, definition.ordinalValue),
                definition,
                stmt.getTableOptions()));
      }
    }

    return partitionedStmtList;
  }
}
