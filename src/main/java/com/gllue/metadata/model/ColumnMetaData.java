package com.gllue.metadata.model;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.common.io.stream.StreamOutput;
import com.gllue.metadata.AbstractMetaData;
import com.gllue.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ColumnMetaData extends AbstractMetaData {
  @Getter private final String name;
  @Getter private final ColumnType type;
  @Getter private final boolean nullable;
  @Getter private final String defaultValue;

  public ColumnMetaData(
      final String name,
      final ColumnType type,
      final boolean nullable,
      final String defaultValue,
      final int version) {
    super(name, version);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(name), "Column name cannot be null or empty.");
    Preconditions.checkNotNull(type, "Column type cannot be null.");

    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
  }

  public void writeTo(final StreamOutput output) {
    super.writeTo(output);
    output.writeStringNul(name);
    output.writeByte((byte) type.getId());
    output.writeBoolean(nullable);
    output.writeNullableString(defaultValue);
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<ColumnMetaData> {
    @Setter private String name;
    @Setter private ColumnType type;
    @Setter private boolean nullable;
    @Setter private String defaultValue;

    public Builder setVersion(final int version) {
      this.version = version;
      return this;
    }

    @Override
    public void readStream(StreamInput input) {
      super.readStream(input);
      this.name = input.readStringNul();
      this.type = ColumnType.getColumnType(input.readByte());
      this.nullable = input.readBoolean();
      this.defaultValue = input.readNullableString();
    }

    @Override
    public void copyFrom(ColumnMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.type = metadata.getType();
      this.nullable = metadata.isNullable();
      this.defaultValue = metadata.getDefaultValue();
    }

    @Override
    public ColumnMetaData build() {
      return new ColumnMetaData(name, type, nullable, defaultValue, version);
    }
  }
}
