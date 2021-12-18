package com.gllue.myproxy.common.util;

import static com.gllue.myproxy.common.util.SQLStatementUtils.columnDefaultExpr;
import static com.gllue.myproxy.common.util.SQLStatementUtils.columnIsNullable;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getForeignKeyReferencingColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getIndexColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getPrimaryKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getUniqueKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.isColumnDefinitionEquals;
import static com.gllue.myproxy.common.util.SQLStatementUtils.isDataTypeNameEquals;
import static com.gllue.myproxy.common.util.SQLStatementUtils.isEncryptColumnType;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newAlterTableStatement;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newColumnDefinition;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newCreateTableStatement;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newDropColumnItem;
import static com.gllue.myproxy.common.util.SQLStatementUtils.newPrimaryKey;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.updateEncryptToVarbinary;
import static com.gllue.myproxy.common.util.SQLStatementUtils.visitColumn;
import static com.gllue.myproxy.common.util.SQLStatementUtils.visitColumns;
import static org.junit.Assert.assertArrayEquals;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLStatementUtilsTest {
  final SQLParser sqlParser = new SQLParser();

  @Test
  public void testQuoteName() {
    assertEquals("`name`", quoteName("name"));
    assertEquals("`name", quoteName("`name"));
    assertEquals("name`", quoteName("name`"));
    assertEquals("`name`", quoteName("`name`"));
  }

  @Test
  public void testUnquoteName() {
    assertEquals("name", unquoteName("`name`"));
    assertEquals("name", unquoteName("`name"));
    assertEquals("name", unquoteName("name`"));
    assertEquals("name", unquoteName("name"));
  }

  @Test
  public void testColumnIsNullable() {
    var columnDef = new SQLColumnDefinition();
    columnDef.setName("c");
    assertTrue(columnIsNullable(columnDef));
    columnDef.addConstraint(new SQLNotNullConstraint());
    assertFalse(columnIsNullable(columnDef));
  }

  @Test
  public void testColumnDefaultExpr() {
    var columnDef = new SQLColumnDefinition();
    columnDef.setName("c");
    assertNull(columnDefaultExpr(columnDef));
    columnDef.setDefaultExpr(new SQLCharExpr("1234"));
    assertEquals("1234", columnDefaultExpr(columnDef));
  }

  @Test
  public void testVisitColumn() {
    assertEquals("column", visitColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("column"))));
    assertEquals(
        "column", visitColumn(new SQLSelectOrderByItem(new SQLMethodInvokeExpr("column"))));
    assertEquals("column", visitColumn(new SQLIdentifierExpr("column")));
  }

  @Test
  public void testVisitColumns() {
    assertArrayEquals(
        new String[] {"c1", "c2", "c3"},
        visitColumns(
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
    assertArrayEquals(new String[] {"c1", "c2"}, getPrimaryKeyColumns(pk));
  }

  @Test
  public void testGetIndexColumns() {
    var index = new MySqlTableIndex();
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")));
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")));
    assertArrayEquals(new String[] {"c1", "c2"}, getIndexColumns(index));
  }

  @Test
  public void testGetUniqueKeyColumns() {
    var index = new MySqlUnique();
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c1")));
    index.addColumn(new SQLSelectOrderByItem(new SQLIdentifierExpr("c2")));
    assertArrayEquals(new String[] {"c1", "c2"}, getUniqueKeyColumns(index));
  }

  @Test
  public void testGetForeignKeyReferencingColumns() {
    var fk = new MysqlForeignKey();
    fk.getReferencingColumns().add(new SQLIdentifierExpr("c1"));
    fk.getReferencingColumns().add(new SQLIdentifierExpr("c2"));
    assertArrayEquals(new String[] {"c1", "c2"}, getForeignKeyReferencingColumns(fk));
  }

  @Test
  public void testNewPrimaryKey() {
    assertArrayEquals(new String[] {"c1", "c2"}, getPrimaryKeyColumns(newPrimaryKey("c1", "c2")));
  }

  @Test
  public void testNewColumnDefinition() {
    var columnDef = newColumnDefinition("column", ColumnType.CHAR, false, "121", "test");
    assertEquals("`column`", columnDef.getColumnName());
    assertTrue(isDataTypeNameEquals(columnDef.getDataType(), ColumnType.CHAR));
    assertFalse(columnIsNullable(columnDef));
    assertEquals("121", columnDefaultExpr(columnDef));
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
        newCreateTableStatement(
            stmt.getTableName(),
            stmt.getTableElementList().stream()
                .filter(x -> x instanceof SQLColumnDefinition)
                .collect(Collectors.toList()),
            newPrimaryKey("id"),
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

    var newStmt = newAlterTableStatement(stmt.getTableName(), stmt.isIgnore(), stmt.getItems());
    AssertUtils.assertSQLEquals(alterTableQuery, newStmt);
  }

  @Test
  public void testNewDropColumnItem() {
    var item = newDropColumnItem("col");
    assertEquals("DROP COLUMN `col`", item.toString());
  }

  @Test
  public void testDataTypeNameEquals() {
    assertTrue(isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.CHAR.name()), ColumnType.CHAR));
    assertTrue(
        isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.DECIMAL.name()), ColumnType.DECIMAL));
    assertFalse(
        isDataTypeNameEquals(new SQLDataTypeImpl(ColumnType.CHAR.name()), ColumnType.VARCHAR));
  }

  @Test
  public void testIsColumnDefinitionEquals() {
    assertTrue(
        isColumnDefinitionEquals(
            newColumnDefinition("col1", ColumnType.CHAR, true, null, null),
            newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        isColumnDefinitionEquals(
            newColumnDefinition("col1", ColumnType.INT, true, null, null),
            newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        isColumnDefinitionEquals(
            newColumnDefinition("col1", ColumnType.CHAR, false, null, null),
            newColumnDefinition("col1", ColumnType.CHAR, true, null, null)));

    assertFalse(
        isColumnDefinitionEquals(
            newColumnDefinition("col1", ColumnType.CHAR, false, "", null),
            newColumnDefinition("col1", ColumnType.CHAR, false, null, null)));

    assertFalse(
        isColumnDefinitionEquals(
            newColumnDefinition("col1", ColumnType.CHAR, false, "", null),
            newColumnDefinition("col1", ColumnType.CHAR, false, null, null)));

    var col1 = newColumnDefinition("col1", ColumnType.CHAR, false, "", null);
    var col2 = col1.clone();
    col1.setCollateExpr(new SQLIdentifierExpr("utf8mb4"));
    assertFalse(isColumnDefinitionEquals(col1, col2));
  }

  @Test
  public void testIsEncryptColumnType() {
    assertFalse(isEncryptColumnType(new SQLDataTypeImpl(ColumnType.CHAR.name())));
    assertTrue(isEncryptColumnType(new SQLDataTypeImpl(ColumnType.ENCRYPT.name())));
  }

  @Test
  public void testUpdateEncryptToVarbinary() {
    var columnDef = newColumnDefinition("col", ColumnType.ENCRYPT, false, "", null);
    var newColumnDef = updateEncryptToVarbinary(columnDef);
    assertEquals(ColumnType.VARBINARY.name(), newColumnDef.getDataType().getName());
  }
}
