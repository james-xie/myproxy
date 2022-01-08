package com.gllue.myproxy.transport.protocol.packet.query.binary.value;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.exception.MalformedPacketException;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class TimeBinaryProtocolValue
    implements BinaryProtocolValue<Timestamp, TimeBinaryProtocolValue> {
  private static final TimeBinaryProtocolValue INSTANCE = new TimeBinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_TIME);
  }

  public TimeBinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Timestamp read(MySQLPayload payload) {
    int length = payload.readInt1();
    payload.readInt1();
    payload.readInt4();
    switch (length) {
      case 0:
        return null;
      case 8:
        return getTimestampForTime(payload);
      case 12:
        return getTimestampForTimeWithMicroSeconds(payload);
      default:
        throw new MalformedPacketException(
            String.format("number of length must be in (0,8,12), got [%s]", length));
    }
  }

  @SuppressWarnings("MagicConstant")
  private Timestamp getTimestampForTime(final MySQLPayload payload) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(
        0, Calendar.JANUARY, 0, payload.readInt1(), payload.readInt1(), payload.readInt1());
    return new Timestamp(calendar.getTimeInMillis());
  }

  private Timestamp getTimestampForTimeWithMicroSeconds(final MySQLPayload payload) {
    var timestamp = getTimestampForTime(payload);
    timestamp.setNanos(payload.readInt4());
    return timestamp;
  }

  @Override
  public void write(MySQLPayload payload, Timestamp value) {
    if (value == null) {
      payload.writeZero(1);
      return;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(value);
    int hours = calendar.get(Calendar.HOUR_OF_DAY);
    int minutes = calendar.get(Calendar.MINUTE);
    int seconds = calendar.get(Calendar.SECOND);
    int nanos = value.getNanos();

    if (nanos > 0) {
      payload.writeInt1(12);
    } else {
      payload.writeInt1(8);
    }

    payload.writeZero(5);
    payload.writeInt1(hours);
    payload.writeInt1(minutes);
    payload.writeInt1(seconds);
    if (nanos > 0) {
      payload.writeInt4(nanos);
    }
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
