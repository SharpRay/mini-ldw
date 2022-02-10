package org.rzlabs.analysis;

import org.rzlabs.common.UserException;

public class DropSchemaStmt implements ParseNode {

    private String schemaName;

    public DropSchemaStmt(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public void analyze() throws UserException {
        if (schemaName.isEmpty()) {
            throw new UserException("schema name cannot be empty");
        }
        if (schemaName.equalsIgnoreCase("default")) {
            throw new UserException("DEFAULT schema is reserved and cannot be dropped");
        }
    }

    @Override
    public String toSql() {
        return "SHOW TABLES";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
