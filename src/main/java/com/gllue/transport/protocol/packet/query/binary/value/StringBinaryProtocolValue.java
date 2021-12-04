package com.gllue.transport.protocol.packet.query.binary.value;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import java.util.HashSet;
import java.util.Set;

public class StringBinaryProtocolValue implements BinaryProtocolValue<String, StringBinaryProtocolValue> {
  private static final StringBinaryProtocolValue INSTANCE = new StringBinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_STRING);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_VARCHAR);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_VAR_STRING);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_ENUM);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_SET);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_LONG_BLOB);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_MEDIUM_BLOB);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_BLOB);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_TINY_BLOB);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_GEOMETRY);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_BIT);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_DECIMAL);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_NEWDECIMAL);
  }

  public StringBinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public String read(MySQLPayload payload) {
    return payload.readStringLenenc();
  }

  @Override
  public void write(MySQLPayload payload, String value) {
    payload.writeStringLenenc(value);
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
