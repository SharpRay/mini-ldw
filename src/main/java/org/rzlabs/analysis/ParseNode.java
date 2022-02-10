package org.rzlabs.analysis;

import org.rzlabs.common.UserException;

public interface ParseNode {

    /**
     * Perform semantic analysis of node and all of its children.
     * Throws exception if any errors found.
     */
    void analyze() throws UserException;

    /**
     * @return SQL syntax corresponding to this node.
     */
    String toSql();
}
