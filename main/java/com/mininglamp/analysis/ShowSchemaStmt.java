package com.mininglamp.analysis;

import com.mininglamp.catalog.Column;
import com.mininglamp.catalog.ScalarType;
import com.mininglamp.common.UserException;
import com.mininglamp.qe.ShowResultSetMetaData;

public class ShowSchemaStmt extends ShowStmt {

    private static final String SCHEMA_COL = "Schema";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(SCHEMA_COL, ScalarType.createVarchar(50)))
                    .build();

    public ShowSchemaStmt() {}

    @Override
    public void analyze() throws UserException {}

    @Override
    public String toSql() {
        return "SHOW SCHEMAS";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
