package com.gllue.transport.protocol.packet.query.binary.value;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class Int2BinaryProtocolValue implements BinaryProtocolValue<Integer, Int2BinaryProtocolValue> {
  private static final Int2BinaryProtocolValue INSTANCE = new Int2BinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_SHORT);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_YEAR);
  }

  public Int2BinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Integer read(MySQLPayload payload) {
    return payload.readInt2();
  }

  @Override
  public void write(MySQLPayload payload, Integer value) {
    payload.writeInt2(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
