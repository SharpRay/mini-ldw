package org.rzlabs.catalog;

import com.google.common.collect.Sets;
import org.rzlabs.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class Catalog {
    private static final Logger LOG = LogManager.getLogger(Catalog.class);
    private static Catalog self = null;

    private Set<String> schemas = Sets.newConcurrentHashSet();

    public Catalog() {
        schemas.add("default");
    }

    public static Catalog currentCatalog() {
        if (self == null) {
            self = new Catalog();
        }
        return self;
    }

    public Set<String> schemaNames() {
        return schemas;
    }

    public synchronized void addSchema(String schemaName) throws Exception {
        if (schemas.contains(schemaName)) {
            throw new UserException("The schema[" + schemaName + "] already exists");
        }
        schemas.add(schemaName);
    }

    public synchronized void dropSchema(String schemaName) throws Exception {
        if (!schemas.contains(schemaName)) {
            throw new UserException("The schema[" + schemaName + "] do not exist");
        }
        schemas.remove(schemaName);
    }
}
