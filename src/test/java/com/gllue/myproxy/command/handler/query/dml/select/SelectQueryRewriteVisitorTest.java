package com.gllue.myproxy.command.handler.query.dml.select;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectQueryRewriteVisitorTest extends BaseQueryHandlerTest {

  @Test
  public void testNothingRewrite() {
    var encryptKey = "";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select *, t3.* from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join table2 t2 on t.id = t2.id "
            + "inner join table3 t3 on t.id = t3.id "
            + "where t.id = 1";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(query, stmt);
    assertFalse(rewriter.isQueryChanged());
  }

  @Test
  public void testExpandStar() {
    var table1 = prepareTable("table1", "id", "col1", "col2", "col3");
    var table2 = prepareTable("table2", "id", "col4", "col5");
    var encryptKey = "";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select *, t3.* from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join table2 t2 on t.id = t2.id "
            + "inner join table3 t3 on t.id = t3.id "
            + "where t.id = 1";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t1.`col2`, t1.`col3`,\n"
            + "t2.`id`, t2.`col4`, t2.`col5`, t3.*, t3.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN table2 t2 ON t.id = t2.id\n"
            + "INNER JOIN table3 t3 ON t.id = t3.id\n"
            + "WHERE t.id = 1",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewriteEncryptColumn() {
    var table1 = prepareTable("table1", "id", "col1");
    var builder = new TableMetaData.Builder();
    builder
        .setName("table2")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table2 = builder.build();
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join table2 t2 on t.id = t2.col2 "
            + "inner join table3 t3 on t.id = t3.id "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t2.`id`, t2.`col1`\n"
            + ", AES_DECRYPT(t2.`col2`, 'key') AS `col2`, t3.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN table2 t2 ON t.id = t2.col2\n"
            + "INNER JOIN table3 t3 ON t.id = t3.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.col2 = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTable() {
    var table1 = prepareTable("table1", "id", "col1");
    var table2 = preparePartitionTable("table2");
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table` `t` "
            + "inner join `table1` `t1` on `t`.`id` = `t1`.`id` "
            + "inner join `table2` `t2` on `t`.`id` = `t2`.`col2` "
            + "inner join `table3` `t3` on `t`.`id` = `t3`.`id` "
            + "where `t`.`id` = 1 and `t2`.`col2` = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT `t`.*, `t1`.`id`, `t1`.`col1`, `t2`.`id`\n"
            + ", AES_DECRYPT(`t2`.`col1`, 'key') AS `col1`, `t2`.`col3`\n"
            + ", AES_DECRYPT(`$ext_0`.`col2`, 'key') AS `col2`, `$ext_0`.`col4`\n"
            + ", `t3`.*\n"
            + "FROM `table` `t`\n"
            + "INNER JOIN `table1` `t1` ON `t`.`id` = `t1`.`id`\n"
            + "INNER JOIN (`table2` `t2`\n"
            + "   LEFT JOIN `db`.`table2_ext_1` `$ext_0` ON `t2`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + ") ON `t`.`id` = `$ext_0`.`col2`\n"
            + "INNER JOIN `table3` `t3` ON `t`.`id` = `t3`.`id`\n"
            + "WHERE `t`.`id` = 1\n"
            + "AND `$ext_0`.`col2` = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableForSubQuery() {
    var table1 = prepareTable("table1", "id", "col1");
    var table2 = preparePartitionTable("table2");
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table1`\n"
            + "inner join (\n"
            + "    select * from `table2`\n"
            + ") t2 on table1.id = t2.col2\n"
            + "where t2.id = 1 and t2.col1 = '1234' and table1.col1 = 'abc'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT `table1`.`id`, `table1`.`col1`, t2.`id`\n"
            + "  , AES_DECRYPT(t2.`col1`, 'key') AS `col1`, t2.`col3`\n"
            + "  , AES_DECRYPT(t2.`col2`, 'key') AS `col2`, t2.`col4`\n"
            + "FROM `table1`\n"
            + "  INNER JOIN (\n"
            + "    SELECT `table2`.`id`, `table2`.`col1`, `table2`.`col3`, `$ext_0`.`col2`, `$ext_0`.`col4`\n"
            + "    FROM `table2`\n"
            + "      LEFT JOIN `db`.`table2_ext_1` `$ext_0` ON `table2`.`$_ext_id` = `$ext_0`.`$_ext_id`\n"
            + "  ) t2\n"
            + "  ON table1.id = t2.col2\n"
            + "WHERE t2.id = 1\n"
            + "  AND t2.col1 = '1234'\n"
            + "  AND table1.col1 = 'abc'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testEncryptColumnInSubQuery() {
    var table1 = prepareTable("table1", "id", "col1");
    var builder = new TableMetaData.Builder();
    builder
        .setName("table2")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table2 = builder.build();
    var table3 = prepareTable("table3", "col3");
    var encryptKey = "key";
    var databasesMetaData =
        prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2, table3);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select * from ("
            + "   select t1.*, t2.col2 as t2_col2 from `table1` t1"
            + "   inner join `table2` t2"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.t2_col2 "
            + "inner join table2 t3 on t.id = t3.id "
            + "inner join table4 t4 on t.id = t4.id "
            + "where t.id = 1 and t2.t2_col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t2.`id`, t2.`col1`\n"
            + ", AES_DECRYPT(t2.`t2_col2`, 'key') AS `t2_col2`, t2.`col3`, t3.`id`, t3.`col1`\n"
            + ", AES_DECRYPT(t3.`col2`, 'key') AS `col2`\n"
            + ", t4.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN (\n"
            + "   SELECT tt1.`id`, tt1.`col1`, tt1.`t2_col2`, t3.`col3`\n"
            + "   FROM (\n"
            + "     SELECT t1.`id`, t1.`col1`, t2.col2 as t2_col2\n"
            + "     FROM `table1` t1\n"
            + "     INNER JOIN `table2` t2\n"
            + "   ) tt1\n"
            + "   INNER JOIN `table3` t3\n"
            + ") t2 ON t.id = t2.t2_col2\n"
            + "INNER JOIN table2 t3 ON t.id = t3.id\n"
            + "INNER JOIN table4 t4 ON t.id = t4.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.t2_col2 = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testEncryptColumnInSubQuery1() {
    var table1 = prepareTable("table1", "id", "col1");
    var builder = new TableMetaData.Builder();
    builder
        .setName("table2")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table2 = builder.build();
    var table3 = prepareTable("table3", "col3");
    var encryptKey = "key";
    var databasesMetaData =
        prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2, table3);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select tt1.col1 as t_col1, tt1.col2 as t_col2, t3.* from ("
            + "   select * from table2"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.t_col2 "
            + "inner join table2 t3 on t.id = t3.id "
            + "inner join table4 t4 on t.id = t4.id "
            + "where t.id = 1 and t2.t_col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t2.`t_col1`\n"
            + ", AES_DECRYPT(t2.`t_col2`, 'key') AS `t_col2`, t2.`col3`\n"
            + ", t3.`id`, t3.`col1`, AES_DECRYPT(t3.`col2`, 'key') AS `col2`\n"
            + ", t4.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN (\n"
            + "   SELECT tt1.col1 AS t_col1, tt1.col2 AS t_col2, t3.`col3`\n"
            + "   FROM (\n"
            + "     SELECT table2.`id`, table2.`col1`, table2.`col2`\n"
            + "     FROM table2\n"
            + "   ) tt1\n"
            + "   INNER JOIN `table3` t3\n"
            + ") t2\n"
            + "ON t.id = t2.t_col2\n"
            + "INNER JOIN table2 t3 ON t.id = t3.id\n"
            + "INNER JOIN table4 t4 ON t.id = t4.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.t_col2 = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test(expected = BadSQLException.class)
  public void testSubQueryRewriteFailed() {
    var table2 = prepareTable("table2", "col1", "col2");
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select * from ("
            + "   select * from `table2`"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.col2 "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
  }

  @Test(expected = BadSQLException.class)
  public void testSubQueryRewriteFailed1() {
    var table1 = prepareTable("table1", "id", "col1");
    var builder = new TableMetaData.Builder();
    builder
        .setName("table2")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table2 = builder.build();
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select * from ("
            + "   select * from `table2`"
            + "   inner join `table3` t3"
            + " ) tt1 "
            + ") t2 on t.id = t2.col2 "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
  }

  @Test
  public void testRewriteForNoTableMetaDataSubQuery() {
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select * from ("
            + "   select * from `table2`"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.col2 "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);

    assertSQLEquals(
        "select t.*, t1.*, t2.* from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select tt1.*, t3.* from ("
            + "   select `table2`.* from `table2`"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.col2 "
            + "where t.id = 1 and t2.col2 = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewriteForUnionQuery() {
    var table1 = prepareTable("table1", "id", "col1");
    var table2 = prepareTable("table2", "id", "col1");
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select * from (\n"
            + "    (\n"
            + "        select * from table1\n"
            + "    ) union (\n"
            + "        select * from table2\n"
            + "    )\n"
            + ") t";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);

    assertSQLEquals(
        "SELECT t.*\n"
            + "FROM (\n"
            + " (SELECT table1.`id`, table1.`col1`\n"
            + "   FROM table1)\n"
            + " UNION\n"
            + " (SELECT table2.`id`, table2.`col1`\n"
            + "   FROM table2)\n"
            + ") t",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testLazyJoinExtensionTable() {
    var table1 = preparePartitionTable("table1");
    var table2 = preparePartitionTable("table2");
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, table1, table2);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(DATABASE, factory, encryptKey);
    var query =
        "select t.* from `table` `t` "
            + "inner join `table1` `t1` on `t`.`id` = `t1`.`id` "
            + "inner join `table2` `t2` on `t`.`id` = `t2`.`col2` "
            + "inner join `table3` `t3` on `t`.`id` = `t3`.`id` "
            + "where `t`.`id` = 1 and `t2`.`col2` = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*\n"
            + "FROM `table` `t`\n"
            + "INNER JOIN `table1` `t1` ON `t`.`id` = `t1`.`id`\n"
            + "INNER JOIN (\n"
            + "  `table2` `t2`\n"
            + "  LEFT JOIN `db`.`table2_ext_1` `$ext_1` ON `t2`.`$_ext_id` = `$ext_1`.`$_ext_id`\n"
            + ") ON `t`.`id` = `$ext_1`.`col2`\n"
            + "INNER JOIN `table3` `t3` ON `t`.`id` = `t3`.`id`\n"
            + "WHERE `t`.`id` = 1\n"
            + "AND `$ext_1`.`col2` = '456'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }
}
