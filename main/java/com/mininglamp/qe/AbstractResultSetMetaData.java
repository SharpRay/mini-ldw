package com.mininglamp.qe;


import com.google.common.base.Preconditions;
import com.mininglamp.catalog.Column;

import java.util.List;

public class AbstractResultSetMetaData implements ResultSetMetaData {
    private final List<Column> columns;

    public AbstractResultSetMetaData(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public Column getColumn(int idx) {
        return columns.get(idx);
    }

    @Override
    public void removeColumn(int idx) {
        Preconditions.checkArgument(idx < columns.size());
        columns.remove(idx);
    }
}
