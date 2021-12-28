package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.gllue.myproxy.common.util.SQLStatementUtils;
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
                + "  `value` LONGTEXT not null COLLATE utf8_bin,\n"
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
    var stmt1 =
        parser.parse(
            ""
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
                + "(null, \"myname\", \"myvalue\");");
    var stmt1 =
        parser.parse(
            "INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6)\n"
                + "  ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);");
    var stmt2 =
        parser.parse(
            "INSERT INTO t1 SET a=1,b=2,c=3\n"
                + "  ON DUPLICATE KEY UPDATE c = 10;");
    var stmt3 =
        parser.parse(
            "INSERT INTO t1 (a, b)\n"
                + "SELECT * FROM\n"
                + "  (SELECT c, d FROM t2\n"
                + "   UNION\n"
                + "   SELECT e, f FROM t3) AS dt\n"
                + "ON DUPLICATE KEY UPDATE b = b + c;");
    var stmt4 =
        parser.parse(
            "INSERT INTO t1\n"
                + "SELECT * FROM\n"
                + "  (SELECT c, d FROM t2\n"
                + "   UNION\n"
                + "   SELECT e, f FROM t3) AS dt\n"
                + "ON DUPLICATE KEY UPDATE b = b + c;");

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
    var stmt1 =
        parser.parse(
            "update candidate t1\n"
                + "inner join candidateexperience t2 on t1.id = t2.candidate_id\n"
                + "set t1.dateAdded = now()");
    var stmt2 =
        parser.parse(
            "update candidate t1\n"
                + "set t1.dateAdded = now()\n"
                + "order by t1.id desc \n"
                + "limit 1;");
    var stmt3 =
        parser.parse(
            "UPDATE items,\n"
                + "       (SELECT id FROM items\n"
                + "        WHERE id IN\n"
                + "            (SELECT id FROM items\n"
                + "             WHERE retail / wholesale >= 1.3 AND quantity < 100))\n"
                + "        AS discounted\n"
                + "SET items.retail = items.retail * 0.9\n"
                + "WHERE items.id = discounted.id;");
    printStatement(stmt);
  }

  @Test
  public void testParseDelete() {
    var parser = newParser();
    var stmt =
        parser.parse(
            "delete from configvalue1 where name = 'company_site' order by id desc limit 1;");
    var stmt1 =
        parser.parse(
            "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3\n"
                + "WHERE t1.id=t2.id AND t2.id=t3.id;");
    var stmt2 =
        parser.parse(
            "DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3\n"
                + "WHERE t1.id=t2.id AND t2.id=t3.id;");
    var stmt3 =
        parser.parse(
            "delete candidate, ext1 \n"
                + "from candidate \n"
                + "left join candidateexperience ext1 on candidate.id = ext1.candidate_id\n"
                + "inner join (\n"
                + "    select candidate.id from candidate\n"
                + "    left join candidateexperience ext1 on candidate.id = ext1.candidate_id\n"
                + "    order by candidate.id desc \n"
                + "    limit 1\n"
                + ") tmp on candidate.id = tmp.id;");
    printStatement(stmt);
  }

  @Test
  public void testParseSelect() {
    var parser = newParser();
    var stmt =
        (SQLSelectStatement)
            parser.parse(
                "select *, db.t1.*, t1.* from a t1 "
                    + "inner join db.b t2 on t1.id = t2.fk_id "
                    + "inner join (select * from c) t3 on t1.id = t3.fk_id "
                    + "inner join db.c t4 on t1.id = t4.fk_id "
                    + "inner join db.d t5 on t1.id = t5.fk_id "
                    + "where t1.name = 'company_site' and t2.value in ( "
                    + "   select value from configvalue "
                    + ") and t2.id in (1,2,3,4,5) "
                    + "group by t1.id "
                    + "order by t1.id desc;");
    //    printStatement(stmt);

    var stmt1 =
        (SQLSelectStatement)
            parser.parse("select `t1.a`.id as tid from elasticjob.jobtask as `t1.a`;");

    var stmt2 = (SQLSelectStatement) parser.parse("select * from ((select 1) union (select 2)) t;");

    var stmt3 =
        (SQLSelectStatement)
            parser.parse(
                "SELECT `Name`, SUM(`Value`)/(SELECT SUM(`Value`) FROM `table1`) AS \"% of Total\"\n"
                    + "FROM `table1`\n"
                    + "WHERE `Year` BETWEEN 2000 AND 2001 and exists (select id from configvalue where id = `table1`.id)\n"
                    + "GROUP BY `Name`;");

    var stmt4 = (SQLSelectStatement) parser.parse("select version();");

    var stmt5 = (SQLSelectStatement) parser.parse(
        "select * from `table` "
            + "where name = '123' and name in (1,2,3) "
            + "and case when mobile = '13911289189' then 1 end "
            + "and name not in (1, '2', '3') "
            + "and name between 1 and 3 "
            + "and name is null and name is not null "
            + "");

    //    var visitor = new SelectQueryInspectVisitor();
    //    stmt.accept(visitor);

    System.out.println(SQLStatementUtils.toSQLString(stmt));
  }
}
