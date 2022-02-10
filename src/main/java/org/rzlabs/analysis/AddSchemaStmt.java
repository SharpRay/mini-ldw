package org.rzlabs.analysis;

import com.google.common.base.Preconditions;
import org.rzlabs.calcite.CalciteSchema;
import org.rzlabs.catalog.Catalog;
import org.rzlabs.common.UserException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AddSchemaStmt implements ParseNode {

    private final String schemaName;
    private final List<String> schemaParams;
    private static final String MYSQL_SOURCE = "mysql";
    private static final String POSTGRESQL_SOURCE = "postgresql";
    private static final String DORIS_SOURCE = "doris";
    private static final String CLICKHOUSE_SOURCE = "clickhouse";
    private static final String ELASTICSEARCH_SOURCE = "elasticsearch";

    public AddSchemaStmt(String schemaName, String schemaParam) {
        this.schemaName = schemaName;
        this.schemaParams = Arrays.asList(schemaParam.split(":", -1)).stream()
                .map(String::trim).collect(Collectors.toList());
    }

    @Override
    public void analyze() throws UserException {
        if (StringUtils.isEmpty(schemaName)) {
            throw new UserException("schema name cannot be empty");
        }
        if (schemaParams.isEmpty()) {
            throw new UserException("schema params cannot be empty");
        }
        if (schemaName.equalsIgnoreCase("default")) {
            throw new UserException("DEFAULT schema is reserved, cannot be added");
        }
    }

    @Override
    public String toSql() {
        return "";
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void addSchema() throws Exception {
        // jdbc: type:host:port:database:username:password
        // elasticsearch: type:host:port:index:username:password
        String type = schemaParams.get(0);
        if (type.equalsIgnoreCase(MYSQL_SOURCE) || type.equalsIgnoreCase(POSTGRESQL_SOURCE) ||
                type.equalsIgnoreCase(DORIS_SOURCE) || type.equalsIgnoreCase(CLICKHOUSE_SOURCE)) {
            Preconditions.checkState(schemaParams.size() >= 6,
                    "Add JDBC schema usage: ADD SCHEMA 'type:host:port:username:password'");
            String jdbcUrlPattern = "jdbc:%s://%s:%d/%s";
            String host = schemaParams.get(1);
            int port = Integer.parseInt(schemaParams.get(2));
            String dbName = schemaParams.get(3);
            String username = schemaParams.get(4);
            String password = schemaParams.get(5);
            String driverName;
            String jdbcUrl;
            switch (type.toLowerCase()) {
                case MYSQL_SOURCE:
                case DORIS_SOURCE:
                    driverName = "com.mysql.jdbc.Driver";
                    jdbcUrl = String.format(jdbcUrlPattern, "mysql", host, port, dbName);
                    break;
                case POSTGRESQL_SOURCE:
                    driverName = "org.postgresql.Driver";
                    jdbcUrl = String.format(jdbcUrlPattern, "postgresql", host, port, dbName);
                    break;
                case CLICKHOUSE_SOURCE:
                    driverName = "com.clickhouse.jdbc.ClickHouseDriver";
                    //driverName = "ru.yandex.clickhouse.ClickHouseDriver";
                    jdbcUrl = String.format(jdbcUrlPattern, "clickhouse", host, port, dbName);
                    break;
                default:
                    throw new RuntimeException("Impossible to reach here");
            }
            CalciteSchema calciteSchema = CalciteSchema.getCalciteSchema();
            calciteSchema.addJdbcSchema(schemaName, jdbcUrl, driverName, username, password);
            Catalog.currentCatalog().addSchema(schemaName);
        } else if (type.equalsIgnoreCase(ELASTICSEARCH_SOURCE)) {
            Preconditions.checkState(schemaParams.size() >= 6,
                    "Add ELASTICSEARCH schema usage: ADD SCHEMA 'type:host:port:index:user:pass'");
            String host = schemaParams.get(1);
            int port = Integer.parseInt(schemaParams.get(2));
            String index = schemaParams.get(3);
            index = index.trim().isEmpty() ? null : index;
            String username = schemaParams.get(4);
            String password = schemaParams.get(5);
            CalciteSchema calciteSchema = CalciteSchema.getCalciteSchema();
            calciteSchema.addElasticsearchSchema(schemaName, host, port, index, username, password);
            Catalog.currentCatalog().addSchema(schemaName);
        } else {
            throw new NotImplementedException("The schema type[" + type + "] is not supported yet.");
        }
    }
}
