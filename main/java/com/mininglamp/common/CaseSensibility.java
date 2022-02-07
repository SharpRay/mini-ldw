package com.mininglamp.common;


public enum CaseSensibility {
    CLUSTER(true),
    DATABASE(true),
    TABLE(true),
    ROLLUP(true),
    PARTITION(true),
    COLUMN(false),
    USER(true),
    ROLE(false),
    HOST(false),
    LABEL(false),
    VARIABLES(true),
    RESOURCE(true),
    CONFIG(true);

    private boolean caseSensitive;

    private CaseSensibility(boolean caseSensitive) {
        this.caseSensitive  = caseSensitive;
    }

    public boolean getCaseSensibility() {
        return caseSensitive;
    }

}
