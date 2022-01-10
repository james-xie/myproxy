package com.gllue.myproxy.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TimeConstants {
  /** Number of nanoseconds in a microsecond. */
  public static final double NANOS_PER_MICRO = 1E3;

  /** Number of nanoseconds in a millisecond. */
  public static final double NANOS_PER_MILLS = 1E6;

  /** Number of nanoseconds in a second. */
  public static final double NANOS_PER_SECOND = 1E9;

  /** Number of milliseconds in a second */
  public static final int MILLS_PER_SECOND = 1000;

  /** Number of seconds in a minute */
  public static final int SECONDS_PER_MINUTE = 60;

  /** Number of seconds in a hour */
  public static final int SECONDS_PER_HOUR = 3600;
}
