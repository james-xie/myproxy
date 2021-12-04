package com.gllue.transport.protocol.packet.query.binary.value;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class Int1BinaryProtocolValue implements BinaryProtocolValue<Integer, Int1BinaryProtocolValue> {

  private static final Int1BinaryProtocolValue INSTANCE = new Int1BinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_TINY);
  }

  public Int1BinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Integer read(MySQLPayload payload) {
    return payload.readInt1();
  }

  @Override
  public void write(MySQLPayload payload, Integer value) {
    payload.writeInt1(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
