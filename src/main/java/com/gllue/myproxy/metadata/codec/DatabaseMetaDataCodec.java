package com.gllue.myproxy.metadata.codec;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;

public class DatabaseMetaDataCodec implements MetaDataCodec<DatabaseMetaData> {
  private static final String MAGIC_NUMBER = "my.proxy.database";
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
    metaData.writeTo(stream);
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
    builder.readStream(stream);
    return builder.build();
  }
}
