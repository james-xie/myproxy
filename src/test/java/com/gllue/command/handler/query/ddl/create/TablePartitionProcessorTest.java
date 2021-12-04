package com.gllue.command.handler.query.ddl.create;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.gllue.AssertUtils;
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

  @Test(expected = Exception.class)
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

  @Test(expected = Exception.class)
  public void testPrepareWithTemporaryTable() {
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

  @Test
  public void testProcessStatements() {
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE))
        .thenReturn(5);
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK))
        .thenReturn(1.0);

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {
              "e_col_1", "e_col_2", "e_col_3", "e_col_4", "e_col_5", "e_col_6", "e_col_7"
            });
    var processor = new TablePartitionProcessor(configurations, attributes);
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
    var stmt = parseCreateTableQuery(query);
    assertTrue(processor.prepare(stmt));
    var stmtList = processor.processStatement(List.of(stmt));
    assertEquals(3, stmtList.size());
    
    AssertUtils.assertSQLEquals(
        "CREATE TABLE `table` (\n"
            + "`id` INT NOT NULL AUTO_INCREMENT,\n"
            + "`p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "`p_col_2` INT(11) NOT NULL,\n"
            + "`p_col_3` INT(11) NOT NULL,\n"
            + "PRIMARY KEY (`id`),\n"
            + "KEY `table_p_col_1` (`p_col_1`),\n"
            + "KEY `table_p_col_1_2` (`p_col_1`, `p_col_2`),\n"
            + "UNIQUE KEY `table_p_col_1_3` (`p_col_1`, `p_col_3`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(0));
    
    AssertUtils.assertSQLEquals(
        "CREATE TABLE `$e_table_1` (\n"
            + "`$_ext_pk` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_1` INT(11) NOT NULL,\n"
            + "`e_col_2` VARCHAR(255) NOT NULL,\n"
            + "`e_col_3` LONGTEXT NULL,\n"
            + "`e_col_4` LONGTEXT NULL,\n"
            + "`e_col_5` VARCHAR(255) NULL,\n"
            + "PRIMARY KEY (`$_ext_pk`),\n"
            + "KEY `table_e_col_1` (`e_col_1`),\n"
            + "KEY `table_e_col_2` (`e_col_2`),\n"
            + "CONSTRAINT `fk_table_col_1` FOREIGN KEY (`e_col_1`) REFERENCES `ext_table` (`id`)"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(1));
    
    AssertUtils.assertSQLEquals(
        "CREATE TABLE `$e_table_2` (\n"
            + "`$_ext_pk` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_6` LONGTEXT NULL,\n"
            + "`e_col_7` LONGTEXT NULL,\n"
            + "PRIMARY KEY (`$_ext_pk`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        stmtList.get(2));
  }
}
