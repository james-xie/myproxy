package com.gllue.metadata.model;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.metadata.AbstractMetaDataBuilder;
import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;

public class TemporaryTableMetaData extends TableMetaData {
  private final List<String> allColumnNames;

  public TemporaryTableMetaData(
      String name, TemporaryColumnMetaData[] columns, List<String> allColumnNames) {
    super(name, name, TableType.TEMPORARY, columns, 0);
    this.allColumnNames = allColumnNames;
  }

  public List<String> getColumnNames() {
    return allColumnNames;
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<TemporaryTableMetaData> {
    @Setter private String name;
    private List<String> allColumnNames = new ArrayList<>();
    private List<TemporaryColumnMetaData> columns = new ArrayList<>();

    @Override
    public void readStream(StreamInput input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copyFrom(TemporaryTableMetaData metadata, CopyOptions options) {
      throw new UnsupportedOperationException();
    }

    public Builder addColumnName(final String columnName) {
      allColumnNames.add(columnName);
      return this;
    }

    public Builder addColumn(final ColumnMetaData column) {
      return addColumn(column.getName(), column);
    }

    public Builder addColumn(final String name, final ColumnMetaData column) {
      if (column instanceof TemporaryColumnMetaData) {
        var tmpColumn = (TemporaryColumnMetaData) column;
        columns.add(
            new TemporaryColumnMetaData.Builder()
                .setOriginTable(tmpColumn.getOriginTable())
                .setOriginName(tmpColumn.getOriginName())
                .setName(name)
                .setType(tmpColumn.getType())
                .setNullable(tmpColumn.isNullable())
                .setDefaultValue(tmpColumn.getDefaultValue())
                .build());
      } else {
        columns.add(
            new TemporaryColumnMetaData.Builder()
                .setOriginTable(column.getTable())
                .setOriginName(column.getName())
                .setName(name)
                .setType(column.getType())
                .setNullable(column.isNullable())
                .setDefaultValue(column.getDefaultValue())
                .build());
      }
      return this;
    }

    public boolean anyColumnExists() {
      return !columns.isEmpty();
    }

    @Override
    public TemporaryTableMetaData build() {
      return new TemporaryTableMetaData(
          name, columns.toArray(new TemporaryColumnMetaData[0]), allColumnNames);
    }
  }
}
