package com.mininglamp.qe;


import java.util.List;

/**
 * Simplified ResultSet implementation
 */
public interface ResultSet {

    /**
     * Moves the cursor froward one row from its current position.
     *
     * @return <code>true</code> if the new current row is valid;
     *<code>false</code> if there are no more rows
     */
    boolean next();

    /**
     * Return result data in list format
     *
     * @return A list of list data of rows
     */
    List<List<String>> getResultRows();

    /**
     * Retrieves the number, types and properties of
     * this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     */
    ResultSetMetaData getMetaData();

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as String
     * @param col the column index
     * @return the column value
     */
    String getString(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as byte
     * @param col the column index
     * @return the column value
     */
    byte getByte(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as short
     * @param col the column index
     * @return the column value
     */
    short getShort(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as int
     * @param col the column index
     * @return the column value
     */
    int getInt(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as long
     * @param col the column index
     * @return the column value
     */
    long getLong(int col);

}
