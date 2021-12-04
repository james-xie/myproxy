package com.gllue.metadata.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ColumnType {
  /**
   * Numeric Data Types.
   *
   * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html"></a>
   */
  INTEGER(1),
  INT(2),
  SMALLINT(3),
  TINYINT(4),
  MEDIUMINT(5),
  BIGINT(6),
  DECIMAL(7),
  NUMERIC(8),
  FLOAT(9),
  DOUBLE(10),
  BIT(11),

  /**
   * Date and Time Data Types
   *
   * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-types.html"></a>
   */
  DATE(21),
  DATETIME(22),
  TIMESTAMP(23),
  TIME(24),
  YEAR(25),

  /**
   * String Data Types
   *
   * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/string-types.html"></a>
   */
  CHAR(41),
  VARCHAR(42),
  BINARY(43),
  VARBINARY(44),
  ENUM(45),
  SET(46),
  TINYTEXT(47),
  TEXT(48),
  MEDIUMTEXT(49),
  LONGTEXT(50),
  TINYBLOB(51),
  BLOB(52),
  MEDIUMBLOB(53),
  LONGBLOB(54),

  /**
   * Spatial Data Types
   *
   * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/spatial-type-overview.html"></a>
   */
  GEOMETRY(61),
  POINT(62),
  LINESTRING(63),
  POLYGON(64),
  MULTIPOINT(65),
  MULTILINESTRING(66),
  MULTIPOLYGON(67),
  GEOMETRYCOLLECTION(68),

  /**
   * The JSON Data Type
   *
   * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/json.html"></a>
   */
  JSON(81),

  /*
   * Custom Column Type
   */
  ENCRYPT(100)
  ;

  private static final Map<Integer, ColumnType> COLUMN_ID_MAP = new HashMap<>();
  private static final Map<String, ColumnType> COLUMN_NAME_MAP = new HashMap<>();

  static {
    for (var item : ColumnType.values()) {
      COLUMN_ID_MAP.put(item.id, item);
      COLUMN_NAME_MAP.put(item.name(), item);
    }
  }

  private final int id;

  public static ColumnType getColumnType(final int value) {
    var type = COLUMN_ID_MAP.get(value);
    if (type != null) {
      return type;
    }
    throw new IllegalArgumentException(String.format("Unknown column type id, [%d]", value));
  }

  public static ColumnType getColumnType(final String value) {
    var type = COLUMN_NAME_MAP.get(value.toUpperCase());
    if (type != null) {
      return type;
    }
    throw new IllegalArgumentException(String.format("Unknown column type id, [%s]", value));
  }
}
