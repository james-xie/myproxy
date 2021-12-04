package com.gllue.command.handler.query.ddl.create;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.AssertUtils;
import com.gllue.command.handler.query.BadCommentAttributeException;
import com.gllue.command.handler.query.BadSQLException;
import com.gllue.command.handler.query.BaseQueryHandlerTest;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigPropertyKey;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TablePartitionProcessorTest extends BaseQueryHandlerTest {

  @Test
  public void testPrepare() {
    mockConfigurations();

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"name", "value"});
    var processor = new TablePartitionProcessor(configurations, attributes);
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` VARBINARY(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertTrue(processor.prepare(parseCreateTableQuery(query)));
  }

  @Test(expected = BadCommentAttributeException.class)
  public void testPrepareWithColumnsNotMatch() {
    mockConfigurations();

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"name", "name1"});
    var processor = new TablePartitionProcessor(configurations, attributes);
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` VARBINARY(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertTrue(processor.prepare(parseCreateTableQuery(query)));
  }

  @Test(expected = BadSQLException.class)
  public void testCreateTableWithPartitionByClause() {
    mockConfigurations();

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"name", "value"});
    var processor = new TablePartitionProcessor(configurations, attributes);
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci "
            + " PARTITION BY RANGE( YEAR(p_col_1) ) (\n"
            + "        PARTITION p0 VALUES LESS THAN (1990),\n"
            + "        PARTITION p1 VALUES LESS THAN (1995),\n"
            + "        PARTITION p2 VALUES LESS THAN (2000),\n"
            + "        PARTITION p3 VALUES LESS THAN (2005)\n"
            + "    );";
    assertTrue(processor.prepare(parseCreateTableQuery(query)));
  }

  @Test(expected = BadSQLException.class)
  public void testCreateTemporaryTable() {
    mockConfigurations();

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"name", "value"});
    var processor = new TablePartitionProcessor(configurations, attributes);
    var query =
        "CREATE TEMPORARY TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` VARBINARY(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertTrue(processor.prepare(parseCreateTableQuery(query)));
  }

  List<MySqlCreateTableStatement> processCreateTableQuery(
      String query, Map<SQLCommentAttributeKey, Object> attributes) {
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE))
        .thenReturn(5);
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK))
        .thenReturn(1.0);

    var processor = new TablePartitionProcessor(configurations, attributes);
    var stmt = parseCreateTableQuery(query);
    assertTrue(processor.prepare(stmt));
    return processor.processStatement(List.of(stmt));
  }

  @Test
  public void testProcessStatements() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {
              "e_col_1", "e_col_2", "e_col_3", "e_col_4", "e_col_5", "e_col_6", "e_col_7"
            });
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` INT(11) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  `e_col_2` VARCHAR(255) NOT NULL,\n"
            + "  `e_col_3` LONGTEXT NULL,\n"
            + "  `e_col_4` LONGTEXT NULL,\n"
            + "  `e_col_5` VARCHAR(255) NULL,\n"
            + "  `e_col_6` LONGTEXT NULL,\n"
            + "  `e_col_7` LONGTEXT NULL,\n"
            + "  primary key (`id`),\n"
            + "  KEY `table_p_col_1` (`p_col_1`),\n"
            + "  KEY `table_p_col_1_2` (`p_col_1`, `p_col_2`),\n"
            + "  KEY `table_e_col_1` (`e_col_1`),\n"
            + "  KEY `table_e_col_2` (`e_col_2`),\n"
            + "  UNIQUE KEY `table_p_col_1_3` (`p_col_1`, `p_col_3`),\n"
            + "  CONSTRAINT `fk_table_col_1` FOREIGN KEY (`e_col_1`) REFERENCES `ext_table` (`id`) \n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    var stmtList = processCreateTableQuery(query, attributes);
    assertEquals(3, stmtList.size());

    AssertUtils.assertSQLEquals(
        "CREATE TABLE `table` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`id` INT NOT NULL AUTO_INCREMENT,\n"
            + "`p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "`p_col_2` INT(11) NOT NULL,\n"
            + "`p_col_3` INT(11) NOT NULL,\n"
            + "PRIMARY KEY (`id`),\n"
            + "KEY `$_key_for_ext_id` (`$_ext_id`),\n"
            + "KEY `table_p_col_1` (`p_col_1`),\n"
            + "KEY `table_p_col_1_2` (`p_col_1`, `p_col_2`),\n"
            + "UNIQUE KEY `table_p_col_1_3` (`p_col_1`, `p_col_3`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(0));

    AssertUtils.assertSQLEquals(
        "CREATE TABLE `$e_table_1` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_1` INT(11) NOT NULL,\n"
            + "`e_col_2` VARCHAR(255) NOT NULL,\n"
            + "`e_col_3` LONGTEXT NULL,\n"
            + "`e_col_4` LONGTEXT NULL,\n"
            + "`e_col_5` VARCHAR(255) NULL,\n"
            + "PRIMARY KEY (`$_ext_id`),\n"
            + "KEY `table_e_col_1` (`e_col_1`),\n"
            + "KEY `table_e_col_2` (`e_col_2`),\n"
            + "CONSTRAINT `fk_table_col_1` FOREIGN KEY (`e_col_1`) REFERENCES `ext_table` (`id`)"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(1));

    AssertUtils.assertSQLEquals(
        "CREATE TABLE `$e_table_2` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_6` LONGTEXT NULL,\n"
            + "`e_col_7` LONGTEXT NULL,\n"
            + "PRIMARY KEY (`$_ext_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(2));
  }

  @Test(expected = BadCommentAttributeException.class)
  public void testExtensionColumnInCommentsDoesNotExists() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"e_col_1"});
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  primary key (`id`),\n"
            + "  KEY `table_key` (`p_col_1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    processCreateTableQuery(query, attributes);
  }

  @Test(expected = BadSQLException.class)
  public void testIndexAcrossTables() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"e_col_1"});
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  primary key (`id`),\n"
            + "  KEY `table_key` (`p_col_1`, `e_col_1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    processCreateTableQuery(query, attributes);
  }

  @Test(expected = BadSQLException.class)
  public void testUnionIndexInExtensionTable() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"e_col_1", "e_col_2"});
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  `e_col_2` INT(11) NOT NULL,\n"
            + "  primary key (`id`),\n"
            + "  KEY `table_key` (`e_col_1`, `e_col_2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    processCreateTableQuery(query, attributes);
  }

  @Test(expected = BadSQLException.class)
  public void testUnionFkInExtensionTable() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"e_col_1", "e_col_2"});
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  `e_col_2` INT(11) NOT NULL,\n"
            + "  primary key (`id`),\n"
            + "  CONSTRAINT `fk_ext_table` FOREIGN KEY (`e_col_1`, `e_col_2`) REFERENCES `ext_table` (`id`) \n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    processCreateTableQuery(query, attributes);
  }

  @Test(expected = BadSQLException.class)
  public void testPrimaryTableNoColumnDefs() {
    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {"e_col_1", "e_col_2"});
    var query =
        "CREATE TABLE `table` (\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  `e_col_2` INT(11) NOT NULL\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    processCreateTableQuery(query, attributes);
  }
}
