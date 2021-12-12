package com.gllue.command.handler.query.dml.select;

import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.PartitionTableMetaData;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SubQueryTreeNode {
  @Getter private final String alias;
  @Getter private final TableScope tableScope;
  private Map<String, SubQueryTreeNode> children = null;
  private Map<String, ColumnMetaData> columnMap = null;

  public void addChild(final SubQueryTreeNode child) {
    if (children == null) {
      children = new HashMap<>();
    }
    var old = children.put(child.alias, child);
    if (old != null) {
      throw new NotUniqueTableOrAliasException(child.alias);
    }
  }

  public SubQueryTreeNode getChild(final String alias) {
    if (children == null) {
      return null;
    }
    return children.get(alias);
  }

  public boolean hasChild(final String alias) {
    return getChild(alias) != null;
  }

  public void addColumn(final String nameOrAlias, final ColumnMetaData column) {
    if (columnMap == null) {
      columnMap = new HashMap<>();
    }
    columnMap.put(nameOrAlias, column);
  }

  public ColumnMetaData getColumn(final String name) {
    if (columnMap == null) {
      return null;
    }
    return columnMap.get(name);
  }

  public ColumnMetaData lookupColumn(final String name) {
    var column = getColumn(name);
    if (column != null) {
      return column;
    }
    if (children != null) {
      for (var child : children.values()) {
        column = child.lookupColumn(name);
        if (column != null) {
          return column;
        }
      }
    }
    return null;
  }

  public boolean anyColumnExists() {
    if (columnMap != null) {
      return true;
    }

    var res = false;
    if (children != null) {
      for (var child : children.values()) {
        res |= child.anyColumnExists();
      }
    }
    return res;
  }

  public static ColumnMetaData lookupColumn(
      final SubQueryTreeNode root, final String alias, final String name) {
    var child = root.getChild(alias);
    if (child == null) {
      return null;
    }
    return child.lookupColumn(name);
  }
}
