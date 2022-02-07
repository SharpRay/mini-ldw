package com.mininglamp.analysis;

import com.mininglamp.common.AnalysisException;
import com.mininglamp.qe.ShowResultSetMetaData;

public abstract class ShowStmt implements ParseNode {

    public abstract ShowResultSetMetaData getMetaData();

//    public SelectStmt toSelectStmt() throws AnalysisException {
//        return null;
//    }
}
