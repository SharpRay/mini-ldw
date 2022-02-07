package com.mininglamp.common;

/**
 * Thrown for errors encountered during analysis of a SQL statement.
 */
public class AnalysisException extends UserException {

    public AnalysisException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AnalysisException(String msg) {
        super(msg);
    }
}
