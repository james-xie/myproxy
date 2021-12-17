package com.gllue.command.handler.query.dml.select;

import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class TableScope {
  @Getter private final TableScope parent;
  private final boolean inherit;

  private Map<String, Map<String, TableMetaData>> databaseTableMap = null;
  private Map<String, Map<String, String[]>> extTableAliasMap = null;

  public void addTable(
      final String database, final String tableOrAlias, final TableMetaData table) {
    assert database != null && tableOrAlias != null && table != null;
    if (databaseTableMap == null) {
      databaseTableMap = new HashMap<>();
    }
    var tableMap = databaseTableMap.computeIfAbsent(database, k -> new HashMap<>());
    var oldValue = tableMap.putIfAbsent(tableOrAlias, table);
    if (oldValue != null) {
      throw new IllegalArgumentException(String.format("Not unique table/alias: %s", tableOrAlias));
    }
  }

  public void addExtensionTableAlias(
      final String database, final String tableOrAlias, final String[] aliases) {
    assert database != null && tableOrAlias != null && aliases != null && aliases.length > 0;
    if (extTableAliasMap == null) {
      extTableAliasMap = new HashMap<>();
    }
    var tableMap = extTableAliasMap.computeIfAbsent(database, k -> new HashMap<>());
    var oldValue = tableMap.putIfAbsent(tableOrAlias, aliases);
    if (oldValue != null) {
      throw new IllegalArgumentException(
          String.format("Not unique table/alias in extension tables: %s", tableOrAlias));
    }
  }

  private interface ScopeGetter<T> {
    T get(TableScope scope, String database, String tableName);
  }

  private <T> T doGetRecursively(
      final String database, final String tableOrAlias, ScopeGetter<T> getter) {
    assert database != null && tableOrAlias != null;

    var scope = this;
    while (scope != null) {
      var table = getter.get(scope, database, tableOrAlias);
      if (table != null) {
        return table;
      }

      if (scope.inherit) {
        scope = scope.parent;
      } else {
        scope = null;
      }
    }
    return null;
  }

  private static TableMetaData getTable0(
      final TableScope scope, final String database, final String tableName) {
    if (scope.databaseTableMap != null) {
      var tableMap = scope.databaseTableMap.get(database);
      if (tableMap != null) {
        return tableMap.get(tableName);
      }
    }
    return null;
  }

  public TableMetaData getTable(final String database, final String tableOrAlias) {
    return doGetRecursively(database, tableOrAlias, TableScope::getTable0);
  }

  private String[] getExtensionTableAliases0(
      final TableScope scope, final String database, final String tableName) {
    if (scope.extTableAliasMap != null) {
      var tableMap = scope.extTableAliasMap.get(database);
      if (tableMap != null) {
        return tableMap.get(tableName);
      }
    }
    return null;
  }

  public String[] getExtensionTableAliases(final String database, final String tableOrAlias) {
    return doGetRecursively(database, tableOrAlias, this::getExtensionTableAliases0);
  }

  public boolean anyTablesInScope() {
    var scope = this;
    while (scope != null) {
      if (scope.databaseTableMap != null) {
        return true;
      }

      if (scope.inherit) {
        scope = scope.parent;
      } else {
        scope = null;
      }
    }
    return false;
  }

  private void findColumnInScope0(
      TableScope scope, String schema, String column, AtomicReference<ColumnMetaData> result) {
    if (scope.databaseTableMap == null) {
      return;
    }

    var tableMap = scope.databaseTableMap.get(schema);
    if (tableMap != null) {
      for (var table : tableMap.values()) {
        if (table.getType() == TableType.EXTENSION || table.getType() == TableType.PRIMARY) {
          continue;
        }

        var columnMetaData = table.getColumn(column);
        if (columnMetaData != null) {
          if (!result.compareAndSet(null, columnMetaData)) {
            throw new AmbiguousColumnException(column, "sql");
          }
        }
      }
    }
    if (scope.inherit && scope.parent != null) {
      findColumnInScope0(scope.parent, schema, column, result);
    }
  }

  public ColumnMetaData findColumnInScope(final String schema, final String column) {
    if (databaseTableMap == null) {
      return null;
    }

    var result = new AtomicReference<ColumnMetaData>();
    findColumnInScope0(this, schema, column, result);
    return result.get();
  }
}
