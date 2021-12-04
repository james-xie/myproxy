package com.gllue.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLParserTest {
  SQLParser newParser() {
    return new SQLParser(true);
  }

  void printStatement(SQLStatement stmt) {
    System.out.println("SQL: ");
    System.out.println(stmt);
    System.out.printf("SQLType: %s\n", stmt.getClass().getSimpleName());
  }

  @Test
  public void testParseComment() {
    var parser = newParser();
    var stmt =
        parser.parse(
            "# {Comment1} \n"
                + "# {Comment2} \n"
                + "CREATE TABLE `configvalue` (\n"
                + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
                + "  `type` VARCHAR(50) DEFAULT 'no type',\n"
                + "  `name` ENCRYPT(255) NOT NULL,\n"
                + "  `value` LONGTEXT not null,\n"
                + "  primary key (`id`),\n"
                + "  unique key `configvalue_type_name` (`type`,`name`),"
                + "  INDEX `idx_name_type` (`name`(10), `type`),"
                + "  FOREIGN KEY fk_config_id(type) REFERENCES config(name)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=12174 DEFAULT CHARSET=utf8mb4 DEFAULT CHARACTER SET=utf8mb4 "
                + "COLLATE=utf8mb4_0900_ai_ci COMMENT='user_behaviour' "
                + "ROW_FORMAT=DEFAULT COMPRESSION=LZ4;");
    printStatement(stmt);
  }

  @Test
  public void testParseCreateTable() {
    var parser = newParser();
    var stmt =
        parser.parse(
            ""
                + "CREATE TABLE `configvalue` (\n"
                + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
                + "  `type` VARCHAR(50) DEFAULT '',\n"
                + "  `name` ENCRYPT(255) NOT NULL,\n"
                + "  `value` LONGTEXT NOT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `configvalue_type_name` (`type`,`name`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=utf8");
    printStatement(stmt);


    var stmt1 =
        parser.parse(
            ""
                + "CREATE TEMPORARY TABLE IF NOT EXISTS `configvalue` (\n"
                + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
                + "  `type` VARCHAR(50) DEFAULT '',\n"
                + "  `name` ENCRYPT(255) NOT NULL,\n"
                + "  `value` LONGTEXT NOT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `configvalue_type_name` (`type`,`name`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=utf8");
    printStatement(stmt1);
  }

  @Test
  public void testParseDropTable() {
    var parser = newParser();
    var stmt = parser.parse("drop TABLE `configvalue1`");
    printStatement(stmt);
  }

  @Test
  public void testParseAlterTableAddColumn() {
    var parser = newParser();
    var stmt =
        parser.parse("" + "alter table configvalue add column `type1` varchar(50) DEFAULT '';");
    printStatement(stmt);
  }

  @Test
  public void testParseAlterTableAddKey() {
    var parser = newParser();
    var stmt1 = parser.parse(""
        + "alter table configvalue "
        + "add column `a` ENCRYPT null,"
        + "add column `b` ENCRYPT null,"
        + "modify column `a` ENCRYPT null,"
        + "change column `a` `b` ENCRYPT null,"
        + "change column `a` `b` INT null,"
        + "drop column `a`,"
        + "rename to `configvalue1`,"
        + "add key `idx_type_name`(`type`, `name`),"
        + "add index `idx_type_name`(`type`, `name`),"
        + "add unique index `idx_type_name`(`type`, `name`)"
        + ";");
    printStatement(stmt1);
  }

  @Test
  public void testParseAlterTableDropColumn() {
    var parser = newParser();
    var stmt = parser.parse("" + "alter table configvalue drop column `type1`;");
    printStatement(stmt);
  }

  @Test
  public void testParseAlterTableModifyColumn() {
    var parser = newParser();
    var stmt = parser.parse("" + "alter table configvalue modify column type1 int(11) default 10;");
    printStatement(stmt);
  }

  @Test
  public void testParseAlterTableChangeColumn() {
    var parser = newParser();
    var stmt =
        parser.parse("alter table configvalue change column type1 type2 int(11) default 10;");
    printStatement(stmt);
  }

  @Test
  public void testParseRenameTable() {
    var parser = newParser();
    var stmt = parser.parse("rename table configvalue to configvalue1;");
    printStatement(stmt);
  }

  @Test
  public void testParseInsert() {
    var parser = newParser();
    var stmt =
        parser.parse(
            "insert into configvalue "
                + "(type, name, value) "
                + "values "
                + "('', \"myname\", \"myvalue\");");
    printStatement(stmt);
  }

  @Test
  public void testParseInsertIntoSelect() {
    var parser = newParser();
    var stmt = parser.parse("insert into configvalue1 select * from configvalue;");
    printStatement(stmt);
  }

  @Test
  public void testParseUpdate() {
    var parser = newParser();
    var stmt = parser.parse("update configvalue1 set value = 0 where name = 'company_site';");
    printStatement(stmt);
  }

  @Test
  public void testParseDelete() {
    var parser = newParser();
    var stmt = parser.parse("delete from configvalue1 where name = 'company_site';");
    printStatement(stmt);
  }

  @Test
  public void testParseSelect() {
    var parser = newParser();
    var stmt = parser.parse("select * from configvalue1 where name = 'company_site';");
    printStatement(stmt);
  }
}
