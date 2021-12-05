package com.gllue.command.handler.query.ddl.alter;

import static com.gllue.common.util.SQLStatementUtils.columnIsNullable;
import static com.gllue.common.util.SQLStatementUtils.isDataTypeNameEquals;
import static com.gllue.common.util.SQLStatementUtils.newColumnDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.gllue.command.handler.query.BaseQueryHandlerTest;
import com.gllue.metadata.model.ColumnType;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EncryptColumnProcessorTest extends BaseQueryHandlerTest {
  EncryptColumnProcessor prepareProcessor(Map<SQLCommentAttributeKey, Object> attributes) {
    return new EncryptColumnProcessor(attributes);
  }

  @Test
  public void testUpdateEncryptToVarbinaryForAdd() {
    var processor = prepareProcessor(Map.of());
    var item = new SQLAlterTableAddColumn();
    item.addColumn(newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getColumns().get(0);
    assertTrue(isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(columnIsNullable(columnDef));
  }

  @Test
  public void testUpdateEncryptToVarbinaryForModify() {
    var processor = prepareProcessor(Map.of());
    var item = new MySqlAlterTableModifyColumn();
    item.setNewColumnDefinition(newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getNewColumnDefinition();
    assertTrue(isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(columnIsNullable(columnDef));
  }

  @Test
  public void testUpdateEncryptToVarbinaryForChange() {
    var processor = prepareProcessor(Map.of());
    var item = new MySqlAlterTableChangeColumn();
    item.setNewColumnDefinition(newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null));
    var updatedItem = processor.updateEncryptToVarbinary(item);
    var columnDef = updatedItem.getNewColumnDefinition();
    assertTrue(isDataTypeNameEquals(columnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(columnIsNullable(columnDef));
  }

  @Test
  public void testNewTemporaryVarbinaryColumnDef() {
    var processor = prepareProcessor(Map.of());
    var columnDef = newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    var newColumnDef = processor.newTemporaryVarbinaryColumnDef(columnDef, "tmpCol");
    assertEquals("`tmpCol`", newColumnDef.getColumnName());
    assertTrue(isDataTypeNameEquals(newColumnDef.getDataType(), ColumnType.VARBINARY));
    assertTrue(columnIsNullable(columnDef));
  }

  @Test
  public void testNewTemporaryVarcharColumnDef() {
    var processor = prepareProcessor(Map.of());
    var columnDef = newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var newColumnDef = processor.newTemporaryVarcharColumnDef(columnDef, "tmpCol");
    assertEquals("`tmpCol`", newColumnDef.getColumnName());
    assertTrue(isDataTypeNameEquals(newColumnDef.getDataType(), ColumnType.VARCHAR));
    assertTrue(columnIsNullable(columnDef));
  }

  @Test
  public void testAlterTableAddTemporaryColumnItemForModify() {
    var processor = prepareProcessor(Map.of());
    var item = new MySqlAlterTableModifyColumn();
    var columnDef = newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    item.setNewColumnDefinition(columnDef);
    var newColumnDef = newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var addItem = processor.alterTableAddTemporaryColumnItem(item, newColumnDef);
    assertEquals(newColumnDef, addItem.getColumns().get(0));
  }

  @Test
  public void testAlterTableAddTemporaryColumnItemForChange() {
    var processor = prepareProcessor(Map.of());
    var item = new MySqlAlterTableChangeColumn();
    var columnDef = newColumnDefinition("col", ColumnType.ENCRYPT, true, null, null);
    item.setNewColumnDefinition(columnDef);
    var newColumnDef = newColumnDefinition("col", ColumnType.VARBINARY, true, null, null);
    var addItem = processor.alterTableAddTemporaryColumnItem(item, newColumnDef);
    assertEquals(newColumnDef, addItem.getColumns().get(0));
  }

  @Test
  public void testGenerateUpdateSqlToEncryptAndDecryptData() {
    var processor = prepareProcessor(Map.of(SQLCommentAttributeKey.ENCRYPT_KEY, "123"));
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
    var processor = prepareProcessor(Map.of(SQLCommentAttributeKey.ENCRYPT_KEY, "123"));
    var varcharColumn = newColumnDefinition("col", ColumnType.VARCHAR, false, null, null);
    var varbinaryColumn = newColumnDefinition("col", ColumnType.VARBINARY, false, null, null);
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
        alterSql
    );
  }
}
