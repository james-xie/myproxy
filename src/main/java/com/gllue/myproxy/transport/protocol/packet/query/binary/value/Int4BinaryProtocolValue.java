package com.gllue.myproxy.transport.protocol.packet.query.binary.value;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class Int4BinaryProtocolValue implements BinaryProtocolValue<Integer, Int4BinaryProtocolValue> {

  private static final Int4BinaryProtocolValue INSTANCE = new Int4BinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_INT24);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_LONG);
  }

  public Int4BinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Integer read(MySQLPayload payload) {
    return payload.readInt4();
  }

  @Override
  public void write(MySQLPayload payload, Integer value) {
    payload.writeInt4(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
