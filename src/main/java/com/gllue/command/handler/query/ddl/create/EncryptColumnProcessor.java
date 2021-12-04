package com.gllue.command.handler.query.ddl.create;

import static com.gllue.common.util.SQLStatementUtils.isEncryptColumn;
import static com.gllue.common.util.SQLStatementUtils.updateEncryptToVarbinary;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.command.handler.CommandHandlerException;
import com.gllue.command.handler.query.BadSQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * A processor that handles encrypted columns.
 *
 * <p>The encrypted columns are declared by the ENCRYPT column type.
 *
 * <pre>
 * For example:
 *    CREATE TABLE ... {
 *      ...
 *      `name` ENCRYPT(255) NOT NULL
 *      ...
 *    }
 * </pre>
 */
@RequiredArgsConstructor
class EncryptColumnProcessor {

  private boolean prepared = false;

  boolean prepare(MySqlCreateTableStatement stmt) {
    var columnDefs = stmt.getColumnDefinitions();
    for (var columnDef : columnDefs) {
      if (isEncryptColumn(columnDef)) {
        if (columnDef.getDefaultExpr() != null) {
          throw new BadSQLException(
              String.format(
                  "Cannot set default expression on the encrypt column. [%s]",
                  columnDef.getColumnName()));
        }
        prepared = true;
        break;
      }
    }
    if (!prepared) {
      return false;
    }

    validateStatement(stmt);
    return true;
  }

  private void validateStatement(MySqlCreateTableStatement stmt) {
    if (stmt.getType() != null) {
      throw new BadSQLException("Add encrypt column to a temporary table is not allowed.");
    }
  }

  List<MySqlCreateTableStatement> processStatement(List<MySqlCreateTableStatement> stmtList) {
    if (!prepared) {
      return stmtList;
    }

    var newStmtList = new ArrayList<MySqlCreateTableStatement>();
    for (var stmt : stmtList) {
      var newStmt = stmt.clone();
      var tableElements = newStmt.getTableElementList();
      int elements = tableElements.size();
      for (int i = 0; i < elements; i++) {
        if (!(tableElements.get(i) instanceof SQLColumnDefinition)) {
          continue;
        }

        var columnDef = (SQLColumnDefinition) tableElements.get(i);
        if (isEncryptColumn(columnDef)) {
          // Change the column type 'ENCRYPT' to the 'VARBINARY'.
          tableElements.set(i, updateEncryptToVarbinary(columnDef));
        }
      }

      newStmtList.add(newStmt);
    }
    return newStmtList;
  }
}
