package org.rzlabs.analysis;

import org.rzlabs.qe.ShowResultSetMetaData;

public abstract class ShowStmt implements ParseNode {

    public abstract ShowResultSetMetaData getMetaData();

//    public SelectStmt toSelectStmt() throws AnalysisException {
//        return null;
//    }
}
