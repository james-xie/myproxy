package com.gllue.transport.protocol.packet.query.binary.value;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class FloatBinaryProtocolValue implements BinaryProtocolValue<Float, FloatBinaryProtocolValue> {
  private static final FloatBinaryProtocolValue INSTANCE = new FloatBinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_FLOAT);
  }

  public FloatBinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Float read(MySQLPayload payload) {
    return payload.readFloat();
  }

  @Override
  public void write(MySQLPayload payload, Float value) {
    payload.writeFloat(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
