package com.mininglamp.qe;


import java.util.List;

public abstract class AbstractResultSet implements ResultSet {
    protected ResultSetMetaData metaData;
    protected List<List<String>> resultRows;
    int rowIdx;

    public AbstractResultSet() {
    }

    public AbstractResultSet(ResultSetMetaData metaData, List<List<String>> resultRows) {
        this.metaData = metaData;
        this.resultRows = resultRows;
        rowIdx = -1;
    }

    @Override
    public boolean next() {
        if (rowIdx + 1 >= resultRows.size()) {
            return false;
        }
        rowIdx++;
        return true;
    }

    @Override
    public List<List<String>> getResultRows() {
        return resultRows;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public String getString(int col) {
        return resultRows.get(rowIdx).get(col);
    }

    @Override
    public byte getByte(int col) {
        return Byte.valueOf(getString(col));
    }

    @Override
    public int getInt(int col) {
        return Integer.valueOf(getString(col));
    }

    @Override
    public long getLong(int col) {
        return Long.valueOf(getString(col));
    }

    @Override
    public short getShort(int col) {
        return Short.valueOf(getString(col));
    }

}
