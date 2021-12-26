package com.gllue.myproxy.transport.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The flags of the column definition.
 *
 * @see <a
 *     href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"></a>
 */
@Getter
@RequiredArgsConstructor
public enum MySQLColumnDefinitionFlags {
  // Field can't be NULL
  NOT_NULL_FLAG(1),

  // Field is part of a primary key
  PRI_KEY_FLAG(2),

  // Field is part of a unique key
  UNIQUE_KEY_FLAG(4),

  // Field is part of a key
  MULTIPLE_KEY_FLAG(8),

  // Field is a blob
  BLOB_FLAG(16),

  // Field is unsigned
  UNSIGNED_FLAG(32),

  // Field is zerofill
  ZEROFILL_FLAG(64),

  // None
  BINARY_FLAG(128),

  // field is an enum
  ENUM_FLAG(256),

  // field is a autoincrement field
  AUTO_INCREMENT_FLAG(512),

  // Field is a timestamp
  TIMESTAMP_FLAG(1024),

  // field is a set
  SET_FLAG(2048),

  // Field doesn't have default value
  NO_DEFAULT_VALUE_FLAG(4096),

  // Field is set to NOW on UPDATE
  ON_UPDATE_NOW_FLAG(8192),

  // Field is num (for clients)
  NUM_FLAG(32768),

  // Intern; Part of some key
  PART_KEY_FLAG(16384),

  // Intern: Group field
  GROUP_FLAG(32768),

  // Intern: Used by sql_yacc
  UNIQUE_FLAG(65536),

  // Intern: Used by sql_yacc
  BINCMP_FLAG(131072),

  // Used to get fields in item tree
  GET_FIXED_FIELDS_FLAG(1 << 18),

  // Field part of partition func
  FIELD_IN_PART_FUNC_FLAG(1 << 19),

  // Intern: Field in TABLE object for new version of altered table, which participates in a newly
  // added index
  FIELD_IN_ADD_INDEX(1 << 20),

  // Intern: Field is being renamed
  FIELD_IS_RENAMED(1 << 21),

  // Field storage media, bit 22-23
  FIELD_FLAGS_STORAGE_MEDIA(22),

  // Field column format, bit 24-25
  FIELD_FLAGS_COLUMN_FORMAT(24),

  // Intern: Field is being dropped
  FIELD_IS_DROPPED(1 << 26),

  // Field is explicitly specified as \ NULL by the user
  EXPLICIT_NULL_FLAG(1 << 27),

  // Field will not be loaded in secondary engine
  NOT_SECONDARY_FLAG(1 << 29),

  // Field is explicitly marked as invisible by the user
  FIELD_IS_INVISIBLE(1 << 30);

  private final int value;
}
