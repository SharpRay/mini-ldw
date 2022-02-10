package org.rzlabs.analysis;

import org.rzlabs.catalog.Column;
import org.rzlabs.catalog.ScalarType;
import org.rzlabs.common.UserException;
import org.rzlabs.qe.ShowResultSetMetaData;

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
