package com.gllue.metadata.model;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.common.io.stream.StreamOutput;
import com.gllue.metadata.AbstractMetaData;
import com.gllue.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class TableMetaData extends AbstractMetaData {
  @Getter private final String name;
  @Getter private final TableType type;
  private final ColumnMetaData[] columns;

  private final Map<String, ColumnMetaData> columnMap;

  public TableMetaData(
      final String identity,
      final String name,
      final TableType type,
      final ColumnMetaData[] columns,
      final int version) {
    super(identity, version);

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(name), "Table name cannot be null or empty.");
    Preconditions.checkArgument(columns.length > 0, "Table must have at least one column.");
    Preconditions.checkNotNull(type, "Table type cannot be null.");

    this.name = name;
    this.type = type;
    this.columns = columns;
    this.columnMap = buildColumnMap(columns);
  }

  private Map<String, ColumnMetaData> buildColumnMap(final ColumnMetaData[] columns) {
    var map = new HashMap<String, ColumnMetaData>(columns.length);
    for (var column : columns) {
      var val = map.put(column.getName(), column);
      if (val != null) {
        throw new IllegalArgumentException(
            String.format("Column name [%s] in columns must be unique.", column.getName()));
      }
    }
    return map;
  }

  public boolean hasColumn(final String name) {
    return columnMap.containsKey(name);
  }

  public ColumnMetaData getColumn(final String name) {
    return columnMap.get(name);
  }

  public ColumnMetaData getColumn(final int index) {
    return columns[index];
  }

  public int getNumberOfColumns() {
    return columns.length;
  }

  @Override
  public void writeTo(StreamOutput output) {
    super.writeTo(output);

    output.writeStringNul(name);
    output.writeByte((byte) type.getId());
    output.writeInt(columns.length);
    for (var column : columns) {
      column.writeTo(output);
    }
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<TableMetaData> {
    @Setter private String name;
    @Setter private TableType type;
    private List<ColumnMetaData> columns = new ArrayList<>();

    public Builder setIdentity(final String identity) {
      this.identity = identity;
      return this;
    }

    public Builder setVersion(final int version) {
      this.version = version;
      return this;
    }

    public Builder setNextVersion(final int version) {
      this.version = version + 1;
      return this;
    }

    public Builder addColumn(final ColumnMetaData column) {
      this.columns.add(column);
      return this;
    }

    public Builder removeColumn(final String name) {
      int index = 0;
      for (var column : columns) {
        if (column.getName().equals(name)) {
          break;
        }
        index++;
      }

      if (index < columns.size()) {
        this.columns.remove(index);
      }
      return this;
    }

    @Override
    public void readStream(StreamInput input) {
      super.readStream(input);
      name = input.readStringNul();
      type = TableType.getTableType(input.readByte());
      int columnsSize = input.readInt();
      columns = new ArrayList<>(columnsSize);
      for (int i = 0; i < columnsSize; i++) {
        var columnBuilder = new ColumnMetaData.Builder();
        columnBuilder.readStream(input);
        columns.add(columnBuilder.build());
      }
    }

    @Override
    public void copyFrom(TableMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.type = metadata.getType();
      if (options == CopyOptions.COPY_CHILDREN) {
        this.columns = new ArrayList<>(metadata.getNumberOfColumns());
        for (int i = 0; i < metadata.getNumberOfColumns(); i++) {
          this.columns.add(metadata.getColumn(i));
        }
      }
    }

    @Override
    public TableMetaData build() {
      return new TableMetaData(
          identity, name, type, columns.toArray(new ColumnMetaData[0]), version);
    }
  }
}
