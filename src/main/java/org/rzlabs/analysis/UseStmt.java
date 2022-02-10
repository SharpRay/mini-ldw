package org.rzlabs.analysis;

import org.rzlabs.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UseStmt implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(UseStmt.class);
    private String database;

    public UseStmt(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public String toSql() {
        return "USE `" + database + "`";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void analyze() throws UserException {}
}
