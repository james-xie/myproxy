package com.gllue.myproxy.command.handler.query.ddl.create;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EncryptColumnProcessorTest extends BaseQueryHandlerTest {

  @Test
  public void testPrepare() {
    var processor = new EncryptColumnProcessor();
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` ENCRYPT(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertTrue(processor.prepare(parseCreateTableQuery(query)));

    processor = new EncryptColumnProcessor();
    query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertFalse(processor.prepare(parseCreateTableQuery(query)));
  }


  @Test(expected = BadSQLException.class)
  public void testEncryptColumnWithDefaultExpr() {
    var processor = new EncryptColumnProcessor();
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `name` ENCRYPT(255) NOT NULL default 'abc',\n"
            + "  primary key (`id`)\n"
            + ");";
    processor.prepare(parseCreateTableQuery(query));
  }

  @Test
  public void testProcessStatement() {
    var processor = new EncryptColumnProcessor();
    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` ENCRYPT(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    var stmt = parseCreateTableQuery(query);
    processor.prepare(stmt);

    var stmtList = processor.processStatement(List.of(stmt));
    assertEquals(1, stmtList.size());
    var expectedQuery =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `name` VARBINARY(255) NOT NULL,\n"
            + "  `value` LONGTEXT not null,\n"
            + "  primary key (`id`)\n"
            + ");";
    assertSQLEquals(expectedQuery, stmtList.get(0));
  }
}
