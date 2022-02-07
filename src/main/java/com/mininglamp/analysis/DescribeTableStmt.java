package com.mininglamp.analysis;

import com.mininglamp.catalog.Column;
import com.mininglamp.catalog.ScalarType;
import com.mininglamp.common.UserException;
import com.mininglamp.qe.ShowResultSetMetaData;

public class DescribeTableStmt extends ShowStmt {

    private String dbName;
    private String tableName;

    public DescribeTableStmt(String tableName) {
        this.tableName = tableName.trim();
    }

    public void setDb(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void analyze() throws UserException {
        if (tableName.isEmpty()) {
            throw new UserException("table name cannot be empty");
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

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Field", ScalarType.createVarchar(50)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(50)));
        return builder.build();
    }
}
