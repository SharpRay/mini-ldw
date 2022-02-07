package com.mininglamp.catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Column {
    private static final Logger LOG = LogManager.getLogger(Column.class);

    private String name;
    private Type type;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
