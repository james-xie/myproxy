package com.gllue.myproxy.metadata.codec;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import lombok.RequiredArgsConstructor;

public class DatabaseMetaDataCodec implements MetaDataCodec<DatabaseMetaData> {
  private static final String MAGIC_NUMBER = "my-proxy";
  private static final String PROTOCOL_VERSION = "1.0.0";
  private static final DatabaseMetaDataCodec INSTANCE = new DatabaseMetaDataCodec();

  private DatabaseMetaDataCodec() {}

  public static DatabaseMetaDataCodec getInstance() {
    return INSTANCE;
  }

  @Override
  public void encode(StreamOutput stream, DatabaseMetaData metaData) {
    stream.writeStringNul(MAGIC_NUMBER);
    stream.writeStringNul(PROTOCOL_VERSION);
    var encoder = new Encoder(stream);
    encoder.encode(metaData);
  }

  @Override
  public DatabaseMetaData decode(StreamInput stream) {
    var magicNumber = stream.readStringNul();
    if (!MAGIC_NUMBER.equals(magicNumber)) {
      throw new MetaDataCodecException("Bad magic number. [%s]", magicNumber);
    }
    var protocol_version = stream.readStringNul();
    if (!PROTOCOL_VERSION.equals(protocol_version)) {
      throw new MetaDataCodecException("Bad protocol version. [%s]", protocol_version);
    }

    var builder = new DatabaseMetaData.Builder();
    var decoder = new Decoder(stream);
    decoder.decode(builder);
    return builder.build();
  }

  @RequiredArgsConstructor
  static class Encoder {
    private final StreamOutput output;

    void encode(DatabaseMetaData metaData) {
      output.writeInt(metaData.getVersion());
      output.writeStringNul(metaData.getDatasource());
      output.writeStringNul(metaData.getName());

      int tableSize = metaData.getNumberOfTables();
      output.writeInt(tableSize);
      for (var table : metaData.getTables()) {
        var tableType = table.getType();
        output.writeInt(tableType.getId());
        if (table.getType() == TableType.STANDARD) {
          encode(table);
        } else if (table.getType() == TableType.PARTITION) {
          encode((PartitionTableMetaData) table);
        } else {
          throw new MetaDataCodecException(
              String.format("Unknown table type [%s]", tableType.name()));
        }
      }
    }

    void encode(PartitionTableMetaData metaData) {
      output.writeStringNul(metaData.getIdentity());
      output.writeInt(metaData.getVersion());
      output.writeStringNul(metaData.getName());
      output.writeByte((byte) metaData.getType().getId());

      encode(metaData.getPrimaryTable());

      var extensionTables = metaData.getExtensionTables();
      output.writeInt(extensionTables.length);
      for (var extensionTable : extensionTables) {
        encode(extensionTable);
      }
    }

    void encode(TableMetaData metaData) {
      output.writeStringNul(metaData.getIdentity());
      output.writeInt(metaData.getVersion());
      output.writeStringNul(metaData.getName());
      output.writeByte((byte) metaData.getType().getId());
      var columnSize = metaData.getNumberOfColumns();
      output.writeInt(columnSize);
      for (int i = 0; i < columnSize; i++) {
        encode(metaData.getColumn(i));
      }
    }

    void encode(ColumnMetaData metaData) {
      output.writeStringNul(metaData.getName());
      output.writeByte((byte) metaData.getType().getId());
      output.writeBoolean(metaData.isNullable());
      output.writeNullableString(metaData.getDefaultValue());
      output.writeBoolean(metaData.isBuiltin());
    }
  }

  @RequiredArgsConstructor
  static class Decoder {
    private final StreamInput input;

    void decode(DatabaseMetaData.Builder builder) {
      builder
          .setVersion(input.readInt())
          .setDatasource(input.readStringNul())
          .setName(input.readStringNul());
      int tables = input.readInt();
      for (int i = 0; i < tables; i++) {
        var tableType = TableType.getTableType(input.readInt());
        if (tableType == TableType.STANDARD) {
          var tableBuilder = new TableMetaData.Builder();
          decode(tableBuilder);
          builder.addTable(tableBuilder.build());
        } else if (tableType == TableType.PARTITION) {
          var tableBuilder = new PartitionTableMetaData.Builder();
          decode(tableBuilder);
          builder.addTable(tableBuilder.build());
        } else {
          throw new MetaDataCodecException(
              String.format("Unknown table type [%s]", tableType.name()));
        }
      }
    }

    void decode(PartitionTableMetaData.Builder builder) {
      builder
          .setIdentity(input.readStringNul())
          .setVersion(input.readInt())
          .setName(input.readStringNul());
      var tableType = TableType.getTableType(input.readByte());
      assert tableType == TableType.PARTITION;

      var primaryTableBuilder = new TableMetaData.Builder();
      decode(primaryTableBuilder);
      builder.setPrimaryTable(primaryTableBuilder.build());

      var extensionTableSize = input.readInt();
      for (int i = 0; i < extensionTableSize; i++) {
        var extensionTableBuilder = new TableMetaData.Builder();
        decode(extensionTableBuilder);
        builder.addExtensionTable(extensionTableBuilder.build());
      }
    }

    void decode(TableMetaData.Builder builder) {
      builder
          .setIdentity(input.readStringNul())
          .setVersion(input.readInt())
          .setName(input.readStringNul())
          .setType(TableType.getTableType(input.readByte()));
      var columnSize = input.readInt();
      for (int i = 0; i < columnSize; i++) {
        var columnBuilder = new ColumnMetaData.Builder();
        decode(columnBuilder);
        builder.addColumn(columnBuilder.build());
      }
    }

    void decode(ColumnMetaData.Builder builder) {
      builder
          .setName(input.readStringNul())
          .setType(ColumnType.getColumnType(input.readByte()))
          .setNullable(input.readBoolean())
          .setDefaultValue(input.readNullableString())
          .setBuiltin(input.readBoolean());
    }
  }
}
