package com.gllue.myproxy.command.handler.query.ddl.alter;

import static org.junit.Assert.assertEquals;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.command.handler.query.EncryptionHelper;
import com.gllue.myproxy.command.handler.query.EncryptionHelper.EncryptionAlgorithm;
import com.gllue.myproxy.common.exception.BadColumnException;
import com.gllue.myproxy.common.exception.ColumnExistsException;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.metadata.model.ColumnMetaData.Builder;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AlterTableStatementProcessorTest extends BaseQueryHandlerTest {
  AlterTableStatementProcessor prepareProcessor(
      TableMetaData tableMetaData, Map<String, SQLColumnDefinition> columnsInDatabase) {
    var encryptKey = "123";
    var encryptProcessor =
        new EncryptColumnProcessor(
            EncryptionHelper.newEncryptor(EncryptionAlgorithm.AES, encryptKey),
            EncryptionHelper.newDecryptor(EncryptionAlgorithm.AES, encryptKey));
    return new AlterTableStatementProcessor(tableMetaData, columnsInDatabase, encryptProcessor);
  }

  @Test
  public void testAlterTableAddColumn() {
    var table = prepareTable("table", "col1");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "ADD COLUMN `col2` INT(11) NOT NULL,"
            + "ADD COLUMN `col3` ENCRYPT NOT NULL,"
            + "ADD COLUMN `col4` VARCHAR(255) NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "ADD COLUMN `col2` INT(11) NOT NULL,"
            + "ADD COLUMN `col3` VARBINARY NOT NULL,"
            + "ADD COLUMN `col4` VARCHAR(255) NULL",
        newStmt);
  }

  @Test(expected = ColumnExistsException.class)
  public void testAlterTableAddColumnWhenColumnInTable() {
    var table = prepareTable("table", "col1");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` ADD COLUMN `col1` INT(11) NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    processor.processStatement(stmt);
  }

  @Test
  public void testAlterTableAddColumnWhenColumnInDatabase() {
    var table = prepareTable("table", "col1");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.VARBINARY, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "ADD COLUMN `col2` ENCRYPT NOT NULL,"
            + "ADD COLUMN `col3` VARCHAR(255) NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals("ALTER TABLE `table` ADD COLUMN `col3` VARCHAR(255) NULL", newStmt);
  }

  @Test
  public void testAlterTableAddColumnWhenColumnInDatabase1() {
    var table = prepareTable("table", "col1");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.VARBINARY, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "ADD COLUMN `col2` ENCRYPT NULL,"
            + "ADD COLUMN `col3` VARCHAR(255) NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "DROP COLUMN `col2`, "
            + "ADD COLUMN `col2` VARBINARY NULL, "
            + "ADD COLUMN `col3` VARCHAR(255) NULL",
        newStmt);
  }

  @Test
  public void testProcessModifyColumn() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(true).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(true).build());
    var table = builder.build();

    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null),
            "col3",
            SQLStatementUtils.newColumnDefinition("col3", ColumnType.ENCRYPT, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "MODIFY COLUMN `col1` ENCRYPT NOT NULL,"
            + "MODIFY COLUMN `col2` BIGINT NULL,"
            + "MODIFY COLUMN `col3` VARCHAR NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "ADD COLUMN `$tmp_col1` VARBINARY NOT NULL, "
            + "MODIFY COLUMN `col2` BIGINT NULL, "
            + "ADD COLUMN `$tmp_col3` VARCHAR NOT NULL",
        newStmt);
  }

  @Test(expected = BadColumnException.class)
  public void testProcessModifyColumnWhenColumnNotInTable() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` MODIFY COLUMN `col1` ENCRYPT NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    processor.processStatement(stmt);
  }

  @Test
  public void testProcessModifyColumnWhenColumnInDatabase() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARBINARY, false, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null),
            "col3",
            SQLStatementUtils.newColumnDefinition("col3", ColumnType.VARCHAR, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "MODIFY COLUMN `col1` ENCRYPT NOT NULL,"
            + "MODIFY COLUMN `col2` BIGINT NULL,"
            + "MODIFY COLUMN `col3` VARCHAR NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals("ALTER TABLE `table` MODIFY COLUMN `col2` BIGINT NULL", newStmt);
  }

  @Test
  public void testProcessModifyColumnWhenColumnInDatabase1() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARBINARY, false, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null),
            "col3",
            SQLStatementUtils.newColumnDefinition("col3", ColumnType.VARCHAR, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "MODIFY COLUMN `col1` ENCRYPT NULL,"
            + "MODIFY COLUMN `col2` BIGINT NULL,"
            + "MODIFY COLUMN `col3` VARCHAR NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "MODIFY COLUMN `col1` VARBINARY NULL, "
            + "MODIFY COLUMN `col2` BIGINT NULL, "
            + "MODIFY COLUMN `col3` VARCHAR NULL",
        newStmt);
  }

  @Test
  public void testProcessChangeColumn() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(true).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(true).build());
    var table = builder.build();

    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null),
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null),
            "col3",
            SQLStatementUtils.newColumnDefinition("col3", ColumnType.ENCRYPT, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "CHANGE COLUMN `col1` `col_1` ENCRYPT NOT NULL,"
            + "CHANGE COLUMN `col2` `col_2` BIGINT NULL,"
            + "CHANGE COLUMN `col3` `col_3` VARCHAR NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "ADD COLUMN `$tmp_col_1` VARBINARY NOT NULL, "
            + "CHANGE COLUMN `col2` `col_2` BIGINT NULL, "
            + "ADD COLUMN `$tmp_col_3` VARCHAR NOT NULL",
        newStmt);
  }

  @Test(expected = BadColumnException.class)
  public void testProcessChangeColumnWhenColumnNotInTable() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col2",
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.INT, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` CHANGE COLUMN `col1` `col2` ENCRYPT NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    processor.processStatement(stmt);
  }

  @Test
  public void testProcessChangeColumnWhenColumnInDatabase() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col_1",
            SQLStatementUtils.newColumnDefinition("col_1", ColumnType.VARBINARY, false, null, null),
            "col_2",
            SQLStatementUtils.newColumnDefinition("col_2", ColumnType.INT, false, null, null),
            "col_3",
            SQLStatementUtils.newColumnDefinition("col_3", ColumnType.VARCHAR, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "CHANGE COLUMN `col1` `col_1` ENCRYPT NOT NULL,"
            + "CHANGE COLUMN `col2` `col_2` BIGINT NOT NULL,"
            + "CHANGE COLUMN `col3` `col_3` VARCHAR NOT NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals("ALTER TABLE `table` MODIFY COLUMN `col_2` BIGINT NOT NULL", newStmt);
  }

  @Test
  public void testProcessChangeColumnWhenColumnInDatabase1() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table")
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new Builder().setName("col1").setType(ColumnType.VARCHAR).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col2").setType(ColumnType.INT).setNullable(false).build());
    builder.addColumn(
        new Builder().setName("col3").setType(ColumnType.ENCRYPT).setNullable(false).build());
    var table = builder.build();
    var columns =
        Map.of(
            "col_1",
            SQLStatementUtils.newColumnDefinition("col_1", ColumnType.VARBINARY, false, null, null),
            "col_2",
            SQLStatementUtils.newColumnDefinition("col_2", ColumnType.INT, false, null, null),
            "col_3",
            SQLStatementUtils.newColumnDefinition("col_3", ColumnType.VARCHAR, false, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql =
        "ALTER TABLE `table` "
            + "CHANGE COLUMN `col1` `col_1` ENCRYPT NULL,"
            + "CHANGE COLUMN `col2` `col_2` INT NOT NULL,"
            + "CHANGE COLUMN `col3` `col_3` VARCHAR NULL";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "MODIFY COLUMN `col_1` VARBINARY NULL,"
            + "MODIFY COLUMN `col_3` VARCHAR NULL,",
        newStmt);
  }

  @Test
  public void testProcessDropColumn() {
    var table = prepareTable("table", "col1", "col2");
    var columns =
        Map.of(
            "col1",
                SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null),
            "col2",
                SQLStatementUtils.newColumnDefinition(
                    "col2", ColumnType.VARCHAR, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` DROP COLUMN `col1`, DROP COLUMN `col2`";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertSQLEquals("ALTER TABLE `table` DROP COLUMN `col1`, DROP COLUMN `col2`", newStmt);
  }

  @Test(expected = BadColumnException.class)
  public void testProcessDropColumnWhenColumnNotInTable() {
    var table = prepareTable("table", "col1");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` DROP COLUMN `col2`";
    var stmt = parseAlterTableQuery(alterSql);
    processor.processStatement(stmt);
  }

  @Test
  public void testProcessDropColumnWhenColumnNotInDatabase() {
    var table = prepareTable("table", "col1", "col2");
    var columns =
        Map.of(
            "col1",
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.VARCHAR, true, null, null));
    var processor = prepareProcessor(table, columns);
    var alterSql = "ALTER TABLE `table` DROP COLUMN `col2`";
    var stmt = parseAlterTableQuery(alterSql);
    var newStmt = processor.processStatement(stmt);
    assertEquals(0, newStmt.getItems().size());
  }
}
