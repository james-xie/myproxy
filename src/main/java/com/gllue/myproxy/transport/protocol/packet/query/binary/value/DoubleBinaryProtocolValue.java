package com.gllue.myproxy.transport.protocol.packet.query.binary.value;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class DoubleBinaryProtocolValue implements BinaryProtocolValue<Double, DoubleBinaryProtocolValue> {
  private static final DoubleBinaryProtocolValue INSTANCE = new DoubleBinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_DOUBLE);
  }

  public DoubleBinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Double read(MySQLPayload payload) {
    return payload.readDouble();
  }

  @Override
  public void write(MySQLPayload payload, Double value) {
    payload.writeDouble(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
