package org.rzlabs.analysis;

import org.rzlabs.catalog.Column;
import org.rzlabs.catalog.ScalarType;
import org.rzlabs.common.UserException;
import org.rzlabs.qe.ShowResultSetMetaData;

public class ShowTableStmt extends ShowStmt {

    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private String db;

    public ShowTableStmt() {}

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public void analyze() throws UserException {}

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
        builder.addColumn(new Column(NAME_COL_PREFIX + db, ScalarType.createVarchar(50)));
        builder.addColumn(new Column(TYPE_COL, ScalarType.createVarchar(50)));
        return builder.build();
    }
}
