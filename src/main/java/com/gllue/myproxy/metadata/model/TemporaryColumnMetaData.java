package com.gllue.myproxy.metadata.model;

import com.gllue.myproxy.metadata.AbstractMetaDataBuilder;
import java.lang.ref.WeakReference;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class TemporaryColumnMetaData extends ColumnMetaData {
  @Getter private final String originName;
  private final WeakReference<TableMetaData> originTable;

  public TemporaryColumnMetaData(
      final String originName,
      final TableMetaData originTable,
      final String name,
      final ColumnType type,
      final boolean nullable,
      final String defaultValue) {
    super(name, type, nullable, defaultValue, 0, false);
    this.originName = originName;
    this.originTable = new WeakReference<>(originTable);
  }

  public TableMetaData getOriginTable() {
    var table = this.originTable == null ? null : this.originTable.get();
    if (table == null) {
      throw new IllegalStateException("Origin table in column cannot be null.");
    }
    return table;
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<TemporaryColumnMetaData> {
    @Setter private TableMetaData originTable;
    @Setter private String originName;
    @Setter private String name;
    @Setter private ColumnType type;
    @Setter private boolean nullable;
    @Setter private String defaultValue;

    @Override
    public void copyFrom(TemporaryColumnMetaData metadata, CopyOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TemporaryColumnMetaData build() {
      return new TemporaryColumnMetaData(
          originName, originTable, name, type, nullable, defaultValue);
    }
  }
}
