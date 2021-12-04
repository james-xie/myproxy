package com.gllue.transport.protocol.packet.query.binary.value;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.gllue.transport.exception.MalformedPacketException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class DatetimeBinaryProtocolValue
    implements BinaryProtocolValue<Timestamp, DatetimeBinaryProtocolValue> {
  private static final DatetimeBinaryProtocolValue INSTANCE = new DatetimeBinaryProtocolValue();

  private static final Set<MySQLColumnType> SUPPORT_COLUMN_TYPES = new HashSet<>();

  static {
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_DATE);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_DATETIME);
    SUPPORT_COLUMN_TYPES.add(MySQLColumnType.MYSQL_TYPE_TIMESTAMP);
  }

  public DatetimeBinaryProtocolValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Timestamp read(MySQLPayload payload) {
    int length = payload.readInt1();
    switch (length) {
      case 0:
        return null;
      case 4:
        return getTimestampForDate(payload);
      case 7:
        return getTimestampForDatetime(payload);
      case 11:
        return getTimestampForDatetimeWithMicroSeconds(payload);
      default:
        throw new MalformedPacketException(
            String.format("Number of length must be in (0,4,7,11), got [%s]", length));
    }
  }

  @SuppressWarnings("MagicConstant")
  private Timestamp getTimestampForDate(final MySQLPayload payload) {
    Calendar result = Calendar.getInstance();
    result.set(payload.readInt2(), payload.readInt1() - 1, payload.readInt1());
    return new Timestamp(result.getTimeInMillis());
  }

  @SuppressWarnings("MagicConstant")
  private Timestamp getTimestampForDatetime(final MySQLPayload payload) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(
        payload.readInt2(),
        payload.readInt1() - 1,
        payload.readInt1(),
        payload.readInt1(),
        payload.readInt1(),
        payload.readInt1());
    return new Timestamp(calendar.getTimeInMillis());
  }

  private Timestamp getTimestampForDatetimeWithMicroSeconds(final MySQLPayload payload) {
    var timestamp = getTimestampForDatetime(payload);
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

    int length;
    if (nanos > 0) {
      length = 11;
    } else if (hours > 0 || minutes > 0 || seconds > 0) {
      length = 7;
    } else {
      length = 4;
    }
    payload.writeInt1(length);
    payload.writeInt2(calendar.get(Calendar.YEAR));
    payload.writeInt1(calendar.get(Calendar.MONTH) + 1);
    payload.writeInt1(calendar.get(Calendar.DAY_OF_MONTH));
    if (length > 4) {
      payload.writeInt1(hours);
      payload.writeInt1(minutes);
      payload.writeInt1(seconds);
    }
    if (length > 7) {
      payload.writeInt4(nanos);
    }
  }

  @Override
  public Set<MySQLColumnType> supportColumnTypes() {
    return SUPPORT_COLUMN_TYPES;
  }
}
