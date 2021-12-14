package com.gllue.command.handler.query.dml.update;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.command.handler.query.BaseQueryHandlerTest;
import com.gllue.command.handler.query.dml.select.TableScopeFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpdateQueryRewriteVisitorTest extends BaseQueryHandlerTest {
  private static final String ENCRYPT_KEY = "123";

  @Test
  public void testNothingRewriteWithSingleUpdate() {
    var table1 = prepareTable("table1", "id", "col1", "col2", "col3");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query = "update table1 set col1 = col2 where id = 1 order by col3 desc limit 1";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertFalse(rewriter.isQueryChanged());
    assertSQLEquals(query, stmt);
  }

  @Test
  public void testRewritePartitionTableWithSingleUpdate() {
    var table1 = preparePartitionTable("table1");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query =
        "update `db`.`table1` "
            + "set `db`.table1.col1 = table1.col2, "
            + "     table1.col3 = `db`.table1.col2,"
            + "     table1.col2 = table1.col4,"
            + "     table1.col2 = '1234'"
            + "where `db`.table1.id = 1 and table1.col2 = '1234'";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "UPDATE `db`.`table1`\n"
            + "  LEFT JOIN `db`.`ext_table_1` `$ext_0` ON `db`.`table1`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "SET `db`.table1.col1 = `$ext_0`.col2, "
            + "     table1.col3 = AES_DECRYPT(`db`.`$ext_0`.col2, '123'), "
            + "     `$ext_0`.col2 = AES_ENCRYPT(`$ext_0`.col4, '123'), "
            + "     `$ext_0`.col2 = AES_ENCRYPT('1234', '123')\n"
            + "WHERE `db`.table1.id = 1\n"
            + "  AND `$ext_0`.col2 = '1234'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableWithOnlyUpdateExtensionTableColumns() {
    var table1 = preparePartitionTable("table1");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query =
        "update `table1` "
            + "set col2 = col3, "
            + "    col4 = '1234'"
            + "where id = 1 and col2 = '1234'";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "UPDATE `table1`\n"
            + "  LEFT JOIN `db`.`ext_table_1` `$ext_0` ON `table1`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "SET col2 = AES_ENCRYPT(col3, '123'), col4 = '1234', `db`.`table1`.`$_ext_id` = `db`.`table1`.`$_ext_id`\n"
            + "WHERE id = 1\n"
            + "  AND col2 = '1234'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableWithSingleUpdateWithOrderBy() {
    var table1 = preparePartitionTable("table1");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query =
        "update `table1`\n"
            + "set col1 = col3,\n"
            + "    col2 = '1234',\n"
            + "    col3 = col4\n"
            + "where id = 1 and col2 = '1234'\n"
            + "order by id desc\n"
            + "limit 10";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "UPDATE `table1`\n"
            + "  LEFT JOIN `db`.`ext_table_1` `$ext_0` ON `table1`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "  INNER JOIN (\n"
            + "    SELECT `table1`.`$_ext_id`\n"
            + "    FROM `table1`\n"
            + "      LEFT JOIN `db`.`ext_table_1` `$ext_0` ON `table1`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "    WHERE id = 1\n"
            + "      AND col2 = '1234'\n"
            + "    ORDER BY id DESC\n"
            + "    LIMIT 10\n"
            + "  ) `$_sub_query`\n"
            + "  ON `table1`.`$_ext_id` = `$_sub_query`.`$_ext_id`\n"
            + "SET col1 = AES_ENCRYPT(col3, '123'), col2 = AES_ENCRYPT('1234', '123'), col3 = col4",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableWithMultiUpdate() {
    var table1 = preparePartitionTable("table1");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query =
        "update `table1` "
            + "inner join `table2` on `table1`.col2 = `table2`.id "
            + "set `table1`.col1 = table2.name, "
            + "    `table1`.col3 = '456', "
            + "    `table2`.name = 'abc' "
            + "where table1.id = 1 and table1.col2 = '1234'";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "UPDATE `table1`\n"
            + "  LEFT JOIN `db`.`ext_table_1` `$ext_0` ON `table1`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "  INNER JOIN `table2` ON `$ext_0`.col2 = `table2`.id\n"
            + "SET `table1`.col1 = AES_ENCRYPT(table2.name, '123'), `table1`.col3 = '456', `table2`.name = 'abc'\n"
            + "WHERE table1.id = 1\n"
            + "  AND `$ext_0`.col2 = '1234'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableWithMultiUpdate1() {
    var table3 = preparePartitionTable("table3");
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table3);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new UpdateQueryRewriteVisitor(DATABASE, factory, ENCRYPT_KEY);
    var query =
        "update `table1`\n"
            + "inner join `table2` on `table1`.col2 = `table2`.id\n"
            + "inner join (\n"
            + "   select * from ("
            + "     select * from table3\n"
            + "   ) t\n"
            + ") `table3` on table1.id = table3.id\n"
            + "set `table1`.col1 = table3.col1, \n"
            + "    `table1`.col3 = table3.col2, \n"
            + "    `table2`.name = table3.col3 \n"
            + "where table1.id = 1 and table1.col2 = '1234'";
    var stmt = parseUpdateQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "UPDATE `table1`\n"
            + "  INNER JOIN `table2` ON `table1`.col2 = `table2`.id\n"
            + "  INNER JOIN (\n"
            + "    SELECT t.`id`, t.`col1`, t.`col3`, t.`col2`, t.`col4`\n"
            + "    FROM (\n"
            + "      SELECT table3.`id`, table3.`col1`, table3.`col3`, `$ext_0`.`col2`, `$ext_0`.`col4`\n"
            + "      FROM table3\n"
            + "        LEFT JOIN `db`.`ext_table_1` `$ext_0` ON table3.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "    ) t\n"
            + "  ) `table3`\n"
            + "  ON table1.id = table3.id\n"
            + "SET `table1`.col1 = AES_DECRYPT(table3.col1, '123'),\n"
            + "   `table1`.col3 = AES_DECRYPT(table3.col2, '123'),\n"
            + "   `table2`.name = table3.col3\n"
            + "WHERE table1.id = 1\n"
            + "  AND table1.col2 = '1234'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }
}
