package org.rzlabs.analysis;

import org.rzlabs.common.UserException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public class CalciteStmt implements ParseNode {

    private static final SqlParser.Config config = SqlParser.configBuilder()
            .setLex(Lex.MYSQL_ANSI).setCaseSensitive(true)
            .setParserFactory(ServerDdlExecutor.PARSER_FACTORY).build();
    private SqlNode sqlNode;

    public CalciteStmt(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    @Override
    public void analyze() throws UserException {}

    @Override
    public String toSql() {
        return "";
    }

    @Override
    public String toString() {
        return toSql();
    }

    public SqlNode getSqlNode() {
        return sqlNode;
    }

    public static CalciteStmt parseStmt(String originStmt) throws Exception {
        SqlParser parser = SqlParser.create(originStmt, config);
        SqlNode node = parser.parseStmt();
        return new CalciteStmt(node);
    }
}
