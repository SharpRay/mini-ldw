package org.rzlabs.qe;


import org.rzlabs.catalog.Column;

import java.util.List;

/**
 * A ResultSetMetaData object can be used to find out about the types and
 * properties of the columns in a ResultSet
 */
public interface ResultSetMetaData {

    /**
     * Whats the number of columns in the ResultSet?
     * @return the number
     */
    int getColumnCount();

    /**
     * Get all columns
     * @return all the columns as list
     */
    List<Column> getColumns();

    /**
     * Get a column at some index
     * @param idx the index of column
     * @return column data
     */
    Column getColumn(int idx);

    /**
     * Remove a column at some index
     * @param idx the index of column
     * @return column data
     */
    void removeColumn(int idx);
}
