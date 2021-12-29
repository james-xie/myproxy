package com.gllue.myproxy.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLIndex;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyConstraint;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.gllue.myproxy.AssertUtils;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.sql.parser.SQLParser;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLStatementUtilsTest {
  final SQLParser sqlParser = new SQLParser();

  @Test
  public void testQuoteName() {
    Assert.assertEquals("`name`", SQLStatementUtils.quoteName("name"));
    Assert.assertEquals("`name", SQLStatementUtils.quoteName("`name"));
    Assert.assertEquals("name`", SQLStatementUtils.quoteName("name`"));
    Assert.assertEquals("`name`", SQLStatementUtils.quoteName("`name`"));
  }

  @Test
  public void testUnquoteName() {
    Assert.assertEquals("name", SQLStatementUtils.unquoteName("`name`"));
    Assert.assertEquals("name", SQLStatementUtils.unquoteName("`name"));
    Assert.assertEquals("name", SQLStatementUtils.unquoteName("name`"));
    Assert.assertEquals("name", SQLStatementUtils.unquoteName("name"));
  }

  @Test
  public void testColumnIsNullable() {
    var columnDef = new SQLColumnDefinition();
    columnDef.setName("c");
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
    columnDef.addConstraint(new SQLNotNullConstraint());
    assertFalse(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testColumnDefaultExpr() {
    var columnDef = new SQLColumnDefinition();
    columnDef.setName("c");
    assertNull(SQLStatementUtils.columnDefaultExpr(columnDef));
    columnDef.setDefaultExpr(new SQLCharExpr("1234"));
    Assert.assertEquals("'1234'", SQLStatementUtils.columnDefaultExpr(columnDef));
  }

  @Test
  public void testVisitColumn() {
    Assert.assertEquals("column", SQLStatementUtils.visitColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("column"))));
    Assert.assertEquals(
        "column", SQLStatementUtils.visitColumn(new SQLSelectOrderByItem(new SQLMethodInvokeExpr("column"))));
    Assert.assertEquals("column", SQLStatementUtils.visitColumn(new SQLIdentifierExpr("column")));
  }

  @Test
  public void testVisitColumns() {
    Assert.assertArrayEquals(
        new String[] {"c1", "c2", "c3"},
        SQLStatementUtils.visitColumns(
            List.of(
                new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")),
                new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")),
                new SQLSelectOrderByItem(new SQLMethodInvokeExpr("c3")))));
  }

  @Test
  public void testGetPrimaryKeyColumns() {
    var pk = new MySqlPrimaryKey();
    pk.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")));
    pk.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")));
    Assert.assertArrayEquals(new String[] {"c1", "c2"}, SQLStatementUtils.getPrimaryKeyColumns(pk));
  }

  @Test
  public void testGetIndexColumns() {
    var index = new MySqlTableIndex();
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")));
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")));
    Assert.assertArrayEquals(new String[] {"c1", "c2"}, SQLStatementUtils.getIndexColumns(index));
  }

  @Test
  public void testGetUniqueKeyColumns() {
    var index = new MySqlUnique();
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")));
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")));
    Assert.assertArrayEquals(new String[] {"c1", "c2"}, SQLStatementUtils.getUniqueKeyColumns(index));
  }

  @Test
  public void testGetForeignKeyReferencingColumns() {
    var fk = new MysqlForeignKey();
    fk.getReferencingColumns().add(new SQLIdentifierExpr("c1"));
    fk.getReferencingColumns().add(new SQLIdentifierExpr("c2"));
    Assert.assertArrayEquals(new String[] {"c1", "c2"}, SQLStatementUtils.getForeignKeyReferencingColumns(fk));
  }

  @Test
  public void testNewPrimaryKey() {
    Assert.assertArrayEquals(new String[] {"c1", "c2"}, SQLStatementUtils.getPrimaryKeyColumns(
        SQLStatementUtils.newPrimaryKey("c1", "c2")));
  }

  @Test
  public void testNewColumnDefinition() {
    var columnDef = SQLStatementUtils.newColumnDefinition("column", ColumnType.CHAR, false, "121", "test");
    assertEquals("`column`", columnDef.getColumnName());
    assertTrue(SQLStatementUtils.isDataTypeNameEquals(columnDef.getDataType(), ColumnType.CHAR));
    assertFalse(SQLStatementUtils.columnIsNullable(columnDef));
    Assert.assertEquals("'121'", SQLStatementUtils.columnDefaultExpr(columnDef));
    assertEquals("'test'", columnDef.getComment().toString());
  }

  @Test
  public void testNewCreateTableStatement() {
    var createTableQuery =
        "CREATE TABLE `table` (\n"
            + "  `id` int NOT NULL AUTO_INCREMENT,\n"
            + "  `name` varchar(255) NOT NULL,\n"
            + "  `parent` varchar(255) NOT NULL,\n"
            + "  `addedBy_id` int DEFAULT NULL,\n"
            + "  `dateAdded` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',\n"
            + "  `lastUpdateDate` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `table_name` (`name`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
    var stmt = (MySqlCreateTableStatement) sqlParser.parse(createTableQuery);

    var newStmt =
        SQLStatementUtils.newCreateTableStatement(
            stmt.getTableName(),
            stmt.getTableElementList().stream()
                .filter(x -> x instanceof SQLColumnDefinition)
                .collect(Collectors.toList()),
            SQLStatementUtils.newPrimaryKey("id"),
            stmt.getTableElementList().stream()
                .filter(x -> x instanceof SQLIndex && !(x instanceof SQLPrimaryKey))
                .collect(Collectors.toList()),
            stmt.getTableElementList().stream()
                .filter(x -> x instanceof SQLForeignKeyConstraint)
                .collect(Collectors.toList()),
            stmt.getTableOptions(),
            stmt.isIfNotExists());
    AssertUtils.assertSQLEquals(createTableQuery, newStmt);
  }

  @Test
  public void testNewAlterTableStatement() {
    var alterTableQuery =
        "alter table `configvalue` "
            + "add column `a` ENCRYPT null,"
            + "add column `b` ENCRYPT null,"
            + "modify column `a` ENCRYPT null,"
            + "change column `a` `b` ENCRYPT null,"
            + "change column `a` `b` INT null,"
            + "drop column `a`,"
            + "rename to `configvalue1`,"
            + "add key `idx_type_name`(`type`, `name`),"
            + "add index `idx_type_name`(`type`, `name`),"
            + "add unique index `idx_type_name`(`type`, `name`)";
    var stmt = (SQLAlterTableStatement) sqlParser.parse(alterTableQuery);

    var newStmt = SQLStatementUtils.newAlterTableStatement(stmt.getTableName(), stmt.isIgnore(), stmt.getItems());
    AssertUtils.assertSQLEquals(alterTableQuery, newStmt);
  }

  @Test
  public void testNewDropColumnItem() {
    var item = SQLStatementUtils.newDropColumnItem("col");
    assertEquals("DROP COLUMN `col`", item.toString());
  }

  @Test
  public void testDataTypeNameEquals() {
    assertTrue(SQLStatementUtils.isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.CHAR.name()), ColumnType.CHAR));
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.DECIMAL.name()), ColumnType.DECIMAL));
    assertFalse(
        SQLStatementUtils.isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.CHAR.name()), ColumnType.VARCHAR));
  }

  @Test
  public void testIsColumnDefinitionEquals() {
    assertTrue(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, true, null, null),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, true, null, null),
            SQLStatementUtils.newColumnDefinition("col2", ColumnType.CHAR, true, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.INT, true, null, null),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, null, null),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, "", null),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, "", null),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, null, null)));

    assertFalse(
        SQLStatementUtils.isColumnDefinitionEquals(
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, null, "abc"),
            SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, null, null)));

    var col1 = SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, "", null);
    var col2 = col1.clone();
    col1.setCollateExpr(new SQLIdentifierExpr("utf8mb4"));
    assertFalse(SQLStatementUtils.isColumnDefinitionEquals(col1, col2));

    col1 = SQLStatementUtils.newColumnDefinition("col1", ColumnType.CHAR, false, "", null);
    col2 = col1.clone();
    col2.setAutoIncrement(true);
    assertFalse(SQLStatementUtils.isColumnDefinitionEquals(col1, col2));
  }

  @Test
  public void testIsEncryptColumnType() {
    assertFalse(SQLStatementUtils.isEncryptColumnType(new SQLDataTypeImpl(ColumnType.CHAR.name())));
    assertTrue(SQLStatementUtils.isEncryptColumnType(new SQLDataTypeImpl(ColumnType.ENCRYPT.name())));
  }

  @Test
  public void testUpdateEncryptToVarbinary() {
    var columnDef = SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, false, "", null);
    var newColumnDef = SQLStatementUtils.updateEncryptToVarbinary(columnDef);
    assertEquals(ColumnType.VARBINARY.name(), newColumnDef.getDataType().getName());
  }
}
