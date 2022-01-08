package com.gllue.myproxy.metadata.model;

import com.gllue.myproxy.metadata.AbstractMetaData;
import com.gllue.myproxy.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.lang.ref.WeakReference;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ColumnMetaData extends AbstractMetaData {
  private WeakReference<TableMetaData> table;
  @Getter private final String name;
  @Getter private final ColumnType type;
  @Getter private final boolean nullable;
  @Getter private final String defaultValue;
  @Getter private final boolean builtin;

  public ColumnMetaData(
      final String name,
      final ColumnType type,
      final boolean nullable,
      final String defaultValue,
      final int version) {
    this(name, type, nullable, defaultValue, version, false);
  }

  public ColumnMetaData(
      final String name,
      final ColumnType type,
      final boolean nullable,
      final String defaultValue,
      final int version,
      final boolean builtin) {
    super(name, version);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(name), "Column name cannot be null or empty.");
    Preconditions.checkNotNull(type, "Column type cannot be null.");

    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
    this.builtin = builtin;
  }

  public void setTable(final TableMetaData table) {
    if (this.table != null) {
      throw new IllegalStateException("Cannot override table property.");
    }
    this.table = new WeakReference<>(table);
  }

  public TableMetaData getTable() {
    var table = this.table == null ? null : this.table.get();
    if (table == null) {
      throw new IllegalStateException("Table in column cannot be null.");
    }
    return table;
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<ColumnMetaData> {
    @Setter private String name;
    @Setter private ColumnType type;
    @Setter private boolean nullable;
    @Setter private String defaultValue;
    @Setter private boolean builtin = false;

    @Override
    public void copyFrom(ColumnMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.type = metadata.getType();
      this.nullable = metadata.isNullable();
      this.defaultValue = metadata.getDefaultValue();
      this.builtin = metadata.isBuiltin();
    }

    @Override
    public ColumnMetaData build() {
      return new ColumnMetaData(name, type, nullable, defaultValue, version, builtin);
    }
  }
}
