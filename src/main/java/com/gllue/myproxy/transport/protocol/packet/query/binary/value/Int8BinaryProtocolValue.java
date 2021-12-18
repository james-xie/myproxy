package com.gllue.myproxy.transport.protocol.packet.query.binary.value;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class Int8BinaryProtocolValue implements BinaryProtocolValue<Long, Int8BinaryProtocolValue> {

  private static final Int8BinaryProtocolValue INSTANCE = new Int8BinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_LONGLONG);
  }

  public Int8BinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Long read(MySQLPayload payload) {
    return payload.readInt8();
  }

  @Override
  public void write(MySQLPayload payload, Long value) {
    payload.writeInt8(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
