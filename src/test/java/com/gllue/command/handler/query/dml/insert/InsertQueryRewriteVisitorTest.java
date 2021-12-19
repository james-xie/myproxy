package com.gllue.command.handler.query.dml.insert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.command.handler.query.BaseQueryHandlerTest;
import com.gllue.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.common.generator.IdGenerator;
import com.gllue.common.util.RandomUtils;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InsertQueryRewriteVisitorTest extends BaseQueryHandlerTest {

  private int nextId = 0;

  private IdGenerator idGenerator() {
    return () -> ++nextId;
  }

  private InsertQueryRewriteVisitor newRewriter(TableMetaData... tables) {
    var encryptKey = "key";
    var databasesMetaData = prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, tables);
    var factory = new TableScopeFactory(DATASOURCE, DATABASE, databasesMetaData);
    return new InsertQueryRewriteVisitor(
        DATABASE, factory, DATASOURCE, databasesMetaData, idGenerator(), encryptKey);
  }

  @Test
  public void testNothingRewrite() {
    var rewriter = newRewriter();
    var query = "insert into table1 (col1, col2, col3) values (1,2,3), (4,5,6)";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(query, stmt);
    assertFalse(rewriter.isQueryChanged());
  }

  @Test
  public void testRewriteEncryptColumnForInsertIntoValues() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table1")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table1 = builder.build();
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col2`) "
            + "values (1, '123', '456'), (2, 'abc', 'efg'), (3, '123', 'abc')";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col2`)\n"
            + "VALUES (1, '123', AES_ENCRYPT('456', 'key')),\n"
            + "  (2, 'abc', AES_ENCRYPT('efg', 'key')),\n"
            + "  (3, '123', AES_ENCRYPT('abc', 'key'))",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableForInsertIntoValues() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col2`, `col3`, `col4`) "
            + "values (1, '1', '2', '3', '4'), (2, 'a', 'b', 'c', 'd'), (3, '123', 'abc', '456', 'efg')";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    var newInsertQueries = rewriter.getNewInsertQueries();
    assertEquals(2, newInsertQueries.size());
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col3`, `$_ext_id`)\n"
            + "VALUES (1, AES_ENCRYPT('1', 'key'), '3', 1),\n"
            + "  (2, AES_ENCRYPT('a', 'key'), 'c', 2),\n"
            + "  (3, AES_ENCRYPT('123', 'key'), '456', 3)",
        newInsertQueries.get(0));
    assertSQLEquals(
        "INSERT INTO `table1_ext_1` (`col2`, `col4`, `$_ext_id`)\n"
            + "VALUES (AES_ENCRYPT('2', 'key'), '4', 1),\n"
            + "  (AES_ENCRYPT('b', 'key'), 'd', 2),\n"
            + "  (AES_ENCRYPT('abc', 'key'), 'efg', 3)",
        newInsertQueries.get(1));
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewritePartitionTableForInsertIntoValues1() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col3`) "
            + "values (1, '1', '2'), (2, 'a', 'b'), (3, '123', 'abc')";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    var newInsertQueries = rewriter.getNewInsertQueries();
    assertEquals(2, newInsertQueries.size());
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col3`, `$_ext_id`)\n"
            + "VALUES (1, AES_ENCRYPT('1', 'key'), '2', 1),\n"
            + "  (2, AES_ENCRYPT('a', 'key'), 'b', 2),\n"
            + "  (3, AES_ENCRYPT('123', 'key'), 'abc', 3)",
        newInsertQueries.get(0));
    assertSQLEquals(
        "INSERT INTO `table1_ext_1` (`$_ext_id`) VALUES (1), (2), (3)",
        newInsertQueries.get(1));
    assertTrue(rewriter.isQueryChanged());
  }


  @Test(expected = ColumnCountNotMatchValueCountException.class)
  public void testColumnCountNotMatchValueCount() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col3`) "
            + "values (1, '1', '2'), (2, 'a'), (3, '123', 'abc')";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);
  }

  @Test
  public void testInsertIntoValuesWithDuplicateKeyUpdate() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col3`) \n"
            + "values (1, '1', '2'), (2, 'a', 'b'), (3, '123', 'abc') \n"
            + "on duplicate key update `col1`='a', `col3`='b'";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    var newInsertQueries = rewriter.getNewInsertQueries();
    assertEquals(2, newInsertQueries.size());
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col3`, `$_ext_id`)\n"
            + "VALUES (1, AES_ENCRYPT('1', 'key'), '2', 1),\n"
            + "  (2, AES_ENCRYPT('a', 'key'), 'b', 2),\n"
            + "  (3, AES_ENCRYPT('123', 'key'), 'abc', 3) \n"
            + "on duplicate key update `col1`=AES_ENCRYPT('a', 'key'), `col3`='b'",
        newInsertQueries.get(0));
    assertSQLEquals(
        "INSERT INTO `table1_ext_1` (`$_ext_id`) VALUES (1), (2), (3)",
        newInsertQueries.get(1));
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testInsertIntoValuesWithDuplicateKeyUpdate1() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col3`) \n"
            + "values (1, '1', '2'), (2, 'a', 'b'), (3, '123', 'abc') \n"
            + "on duplicate key update `col1`=values(`col1`), `col3`=values(`col3`)";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    var newInsertQueries = rewriter.getNewInsertQueries();
    assertEquals(2, newInsertQueries.size());
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col3`, `$_ext_id`)\n"
            + "VALUES (1, AES_ENCRYPT('1', 'key'), '2', 1),\n"
            + "  (2, AES_ENCRYPT('a', 'key'), 'b', 2),\n"
            + "  (3, AES_ENCRYPT('123', 'key'), 'abc', 3) \n"
            + "on duplicate key update `col1`=values(`col1`), `col3`=values(`col3`)",
        newInsertQueries.get(0));
    assertSQLEquals(
        "INSERT INTO `table1_ext_1` (`$_ext_id`) VALUES (1), (2), (3)",
        newInsertQueries.get(1));
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testInsertIntoValuesWithDuplicateKeyUpdate2() {
    var table1 = preparePartitionTable("table1");
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col3`) \n"
            + "values (1, '1', '2'), (2, 'a', 'b'), (3, '123', 'abc') \n"
            + "on duplicate key update `col1` = values(`col3`), `col3` = values(`col1`)";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    var newInsertQueries = rewriter.getNewInsertQueries();
    assertEquals(2, newInsertQueries.size());
    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col3`, `$_ext_id`)\n"
            + "VALUES (1, AES_ENCRYPT('1', 'key'), '2', 1),\n"
            + "  (2, AES_ENCRYPT('a', 'key'), 'b', 2),\n"
            + "  (3, AES_ENCRYPT('123', 'key'), 'abc', 3) \n"
            + "on duplicate key update\n"
            + "`col1` = AES_ENCRYPT(values(`col3`), 'key'),\n"
            + "`col3` = AES_DECRYPT(values(`col1`), 'key')",
        newInsertQueries.get(0));
    assertSQLEquals(
        "INSERT INTO `table1_ext_1` (`$_ext_id`) VALUES (1), (2), (3)",
        newInsertQueries.get(1));
    assertTrue(rewriter.isQueryChanged());
  }

  @Test
  public void testRewriteEncryptColumnForInsertIntoSelect() {
    var builder = new TableMetaData.Builder();
    builder
        .setName("table1")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build());
    var table1 = builder.build();
    var rewriter = newRewriter(table1);
    var query =
        "insert into `table1` (`id`, `col1`, `col2`) \n"
            + "select `id`, `col2`, `col1` from `table1` \n"
            + "where col1 = '123'";
    var stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col2`)\n"
            + "SELECT `id`, AES_DECRYPT(`col2`, 'key')\n"
            + "\t, AES_ENCRYPT(`col1`, 'key')\n"
            + "FROM `table1`\n"
            + "WHERE col1 = '123'",
        stmt);
    assertTrue(rewriter.isQueryChanged());

    query =
        "insert into `table1` (`id`, `col1`, `col2`) \n"
            + "select `id`, `col1`, '123' from `table1` \n"
            + "where col1 = '123'";
    stmt = parseInsertQuery(query);
    stmt.accept(rewriter);

    assertSQLEquals(
        "INSERT INTO `table1` (`id`, `col1`, `col2`)\n"
            + "SELECT `id`, `col1`, AES_ENCRYPT('123', 'key')\n"
            + "FROM `table1`\n"
            + "WHERE col1 = '123'",
        stmt);
    assertTrue(rewriter.isQueryChanged());
  }
}
