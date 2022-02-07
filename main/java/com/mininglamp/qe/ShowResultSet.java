package com.mininglamp.qe;


import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

// Result set of show statement.
// Redefine ResultSet now, because JDBC is too complicated.
public class ShowResultSet extends AbstractResultSet {

    public ShowResultSet(ResultSetMetaData metaData, List<List<String>> resultRows) {
        super(metaData, resultRows);
    }
}
