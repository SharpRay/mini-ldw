package com.mininglamp.qe;


import com.google.common.collect.Lists;
import com.mininglamp.catalog.Column;

import java.util.List;

public class ShowResultSetMetaData extends AbstractResultSetMetaData {

    public ShowResultSetMetaData(List<Column> columns) {
        super(columns);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public ShowResultSetMetaData build() {
            return new ShowResultSetMetaData(columns);
        }

        public Builder addColumn(Column col) {
            columns.add(col);
            return this;
        }
    }
}
