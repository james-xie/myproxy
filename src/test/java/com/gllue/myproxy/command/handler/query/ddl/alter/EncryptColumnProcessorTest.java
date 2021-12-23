package com.gllue.myproxy.command.handler.query.ddl.alter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EncryptColumnProcessorTest extends BaseQueryHandlerTest {
  EncryptColumnProcessor prepareProcessor(String encryptKey) {
    return new EncryptColumnProcessor(encryptKey);
  }

  @Test
  public void testUpdateEncryptToVarbinaryForAdd() {
    var processor = prepareProcessor(null);
    var item = new SQLAlterTableAddColumn();
    item.addColumn(
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getColumns().get(0);
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testUpdateEncryptToVarbinaryForModify() {
    var processor = prepareProcessor(null);
    var item = new MySqlAlterTableModifyColumn();
    item.setNewColumnDefinition(
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getNewColumnDefinition();
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testUpdateEncryptToVarbinaryForChange() {
    var processor = prepareProcessor(null);
    var item = new MySqlAlterTableChangeColumn();
    item.setNewColumnDefinition(
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getNewColumnDefinition();
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testNewTemporaryVarbinaryColumnDef() {
    var processor = prepareProcessor(null);
    var columnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    var newColumnDef = processor.newTemporaryVarbinaryColumnDef(columnDef, "tmpCol");
    assertEquals("`tmpCol`", newColumnDef.getColumnName());
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(newColumnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testNewTemporaryVarcharColumnDef() {
    var processor = prepareProcessor(null);
    var columnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var newColumnDef = processor.newTemporaryVarcharColumnDef(columnDef, "tmpCol");
    assertEquals("`tmpCol`", newColumnDef.getColumnName());
    assertTrue(
        SQLStatementUtils.isDataTypeNameEquals(newColumnDef.getDataType(), ColumnType.VARCHAR));
    assertTrue(SQLStatementUtils.columnIsNullable(columnDef));
  }

  @Test
  public void testAlterTableAddTemporaryColumnItemForModify() {
    var processor = prepareProcessor(null);
    var item = new MySqlAlterTableModifyColumn();
    var columnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    item.setNewColumnDefinition(columnDef);
    var newColumnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var addItem = processor.alterTableAddTemporaryColumnItem(item, newColumnDef);
    assertEquals(newColumnDef, addItem.getColumns().get(0));
  }

  @Test
  public void testAlterTableAddTemporaryColumnItemForChange() {
    var processor = prepareProcessor(null);
    var item = new MySqlAlterTableChangeColumn();
    var columnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    item.setNewColumnDefinition(columnDef);
    var newColumnDef =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var addItem = processor.alterTableAddTemporaryColumnItem(item, newColumnDef);
    assertEquals(newColumnDef, addItem.getColumns().get(0));
  }

  @Test
  public void testGenerateUpdateSqlToEncryptAndDecryptData() {
    var processor = prepareProcessor("123");
    processor.encryptColumns.add(new EncryptColumnInfo("col1", "col1", "tmpCol1", null));
    processor.encryptColumns.add(new EncryptColumnInfo("col2", "newCol2", "tmpCol2", null));
    processor.decryptColumns.add(new EncryptColumnInfo("col3", "newCol3", "tmpCol3", null));
    processor.decryptColumns.add(new EncryptColumnInfo("col4", "col4", "tmpCol4", null));
    var updateSql = processor.generateUpdateSqlToEncryptAndDecryptData("table");
    assertSQLEquals(
        "UPDATE `table` SET "
            + "`tmpCol1` = AES_ENCRYPT(`col1`, '123'), "
            + "`tmpCol2` = AES_ENCRYPT(`col2`, '123'), "
            + "`tmpCol3` = AES_DECRYPT(`col3`, '123'), "
            + "`tmpCol4` = AES_DECRYPT(`col4`, '123')",
        updateSql);
  }

  @Test
  public void testGenerateAlterSqlToRenameTemporaryColumn() {
    var processor = prepareProcessor("123");
    var varcharColumn =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.VARCHAR, false, null, null);
    var varbinaryColumn =
        SQLStatementUtils.newColumnDefinition("col", ColumnType.VARBINARY, false, null, null);
    processor.encryptColumns.add(new EncryptColumnInfo("col1", "col1", "tmpCol1", varcharColumn));
    processor.encryptColumns.add(
        new EncryptColumnInfo("col2", "newCol2", "tmpCol2", varbinaryColumn));
    processor.decryptColumns.add(
        new EncryptColumnInfo("col3", "newCol3", "tmpCol3", varbinaryColumn));
    processor.decryptColumns.add(new EncryptColumnInfo("col4", "col4", "tmpCol4", varcharColumn));
    var alterSql = processor.generateAlterSqlToRenameTemporaryColumn("table");
    assertSQLEquals(
        "ALTER TABLE `table` "
            + "DROP COLUMN `col1`, CHANGE COLUMN `tmpCol1` `col1` VARCHAR NOT NULL, "
            + "DROP COLUMN `col2`, CHANGE COLUMN `tmpCol2` `newCol2` VARBINARY NOT NULL, "
            + "DROP COLUMN `col3`, CHANGE COLUMN `tmpCol3` `newCol3` VARBINARY NOT NULL, "
            + "DROP COLUMN `col4`, CHANGE COLUMN `tmpCol4` `col4` VARCHAR NOT NULL",
        alterSql);
  }
}
