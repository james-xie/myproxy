package com.gllue.command.handler.query.dml.select;

import com.gllue.command.handler.query.BadSQLException;
import com.gllue.command.handler.query.BaseQueryHandlerTest;
import com.gllue.command.handler.query.TablePartitionHelper;
import com.gllue.common.util.RandomUtils;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectQueryRewriteVisitorTest extends BaseQueryHandlerTest {

  @Test
  public void testNothingRewrite() {
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
    var query =
        "select *, t3.* from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join table2 t2 on t.id = t2.id "
            + "inner join table3 t3 on t.id = t3.id "
            + "where t.id = 1";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(query, stmt);
  }

  @Test
  public void testExpandStar() {
    var table1 = prepareTable("table1", "id", "col1", "col2", "col3");
    var table2 = prepareTable("table2", "id", "col4", "col5");
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
            + ", AES_DECRYPT(t2.`col2`, 'key'), t3.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN table2 t2 ON t.id = t2.col2\n"
            + "INNER JOIN table3 t3 ON t.id = t3.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.col2 = '456'",
        stmt);
  }

  @Test
  public void testRewritePartitionTable() {
    var table1 = prepareTable("table1", "id", "col1");
    var primaryTable =
        new TableMetaData.Builder()
            .setName("table2")
            .setIdentity(RandomUtils.randomShortUUID())
            .setType(TableType.PRIMARY)
            .addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build())
            .addColumn(
                new ColumnMetaData.Builder().setName("col1").setType(ColumnType.ENCRYPT).build())
            .addColumn(
                new ColumnMetaData.Builder()
                    .setName(TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN)
                    .setType(ColumnType.INT)
                    .setBuiltin(true)
                    .build())
            .build();
    var extensionTable =
        new TableMetaData.Builder()
            .setName("ext_table_1")
            .setIdentity(RandomUtils.randomShortUUID())
            .setType(TableType.EXTENSION)
            .addColumn(
                new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build())
            .addColumn(
                new ColumnMetaData.Builder()
                    .setName(TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN)
                    .setType(ColumnType.INT)
                    .setBuiltin(true)
                    .build())
            .build();

    var builder =
        new PartitionTableMetaData.Builder()
            .setName("table2")
            .setIdentity(RandomUtils.randomShortUUID())
            .setPrimaryTable(primaryTable)
            .addExtensionTable(extensionTable);
    var table2 = builder.build();
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
            + ", AES_DECRYPT(`t2`.`col1`, 'key')\n"
            + ", AES_DECRYPT(`$ext_0`.`col2`, 'key'), `t3`.*\n"
            + "FROM `table` `t`\n"
            + "INNER JOIN `table1` `t1` ON `t`.`id` = `t1`.`id`\n"
            + "INNER JOIN ("
            + "`table2` `t2` "
            + "LEFT JOIN `ext_table_1` `$ext_0` ON `t2`.`$_ext_id` = `$ext_0`.`$_ext_id`"
            + ") ON `t`.`id` = `$ext_0`.`col2`\n"
            + "INNER JOIN `table3` `t3` ON `t`.`id` = `t3`.`id`\n"
            + "WHERE `t`.`id` = 1\n"
            + "AND `$ext_0`.`col2` = '456'",
        stmt);
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
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData =
        prepareMultiDatabasesMetaData(datasource, database, table1, table2, table3);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select * from ("
            + "   select t1.*, t2.col2 as t2_col2 from `table1` t1"
            + "   inner join `table2` t2"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.col2 "
            + "inner join table2 t3 on t.id = t3.id "
            + "inner join table4 t4 on t.id = t4.id "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t2.`id`, t2.`col1`\n"
            + ", AES_DECRYPT(t2.`t2_col2`, 'key'), t2.`col3`, t3.`id`, t3.`col1`, AES_DECRYPT(t3.`col2`, 'key')\n"
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
            + ") t2 ON t.id = t2.col2\n"
            + "INNER JOIN table2 t3 ON t.id = t3.id\n"
            + "INNER JOIN table4 t4 ON t.id = t4.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.col2 = '456'",
        stmt);
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
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData =
        prepareMultiDatabasesMetaData(datasource, database, table1, table2, table3);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
    var query =
        "select * from `table` t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join ("
            + " select tt1.col1 as t_col1, tt1.col2 as t_col2, t3.* from ("
            + "   select * from table2"
            + " ) tt1 "
            + " inner join `table3` t3"
            + ") t2 on t.id = t2.col2 "
            + "inner join table2 t3 on t.id = t3.id "
            + "inner join table4 t4 on t.id = t4.id "
            + "where t.id = 1 and t2.col2 = '456'";
    var stmt = parseSelectQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "SELECT t.*, t1.`id`, t1.`col1`, t2.`t_col1`\n"
            + ", AES_DECRYPT(t2.`t_col2`, 'key'), t2.`col3`\n"
            + ", t3.`id`, t3.`col1`, AES_DECRYPT(t3.`col2`, 'key')\n"
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
            + "ON t.id = t2.col2\n"
            + "INNER JOIN table2 t3 ON t.id = t3.id\n"
            + "INNER JOIN table4 t4 ON t.id = t4.id\n"
            + "WHERE t.id = 1\n"
            + "AND t2.col2 = '456'",
        stmt);
  }

  @Test(expected = BadSQLException.class)
  public void testSubQueryRewriteFailed() {
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
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
  public void testRewriteForNoEncryptColumnSubQuery() {
    var table1 = prepareTable("table1", "id", "col1");
    var table2 = prepareTable("table2", "col2");
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
        "SELECT t.*, t1.`id`, t1.`col1`, t2.*\n"
            + "FROM `table` t\n"
            + "INNER JOIN table1 t1 ON t.id = t1.id\n"
            + "INNER JOIN (\n"
            + "   SELECT tt1.`col2`, t3.*\n"
            + "   FROM (\n"
            + "     SELECT `table2`.`col2`\n"
            + "     FROM `table2`\n"
            + "   ) tt1\n"
            + "   INNER JOIN `table3` t3\n"
            + ") t2\n"
            + "ON t.id = t2.col2\n"
            + "WHERE t.id = 1\n"
            + "AND t2.col2 = '456'",
        stmt);
  }

  @Test
  public void testRewriteForUnionQuery() {
    var table1 = prepareTable("table1", "id", "col1");
    var table2 = prepareTable("table2", "id", "col1");
    var datasource = DATASOURCE;
    var database = DATABASE;
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var rewriter = new SelectQueryRewriteVisitor(database, encryptKey, factory);
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
  }
}
