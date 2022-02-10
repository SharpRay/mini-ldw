package org.rzlabs.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.rzlabs.common.UserException;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultIterator;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.StatementCustomizer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalciteSchema {

    private static CalciteSchema INSTANCE = null;
    private SchemaPlus rootSchema;
    private Handle handle;
    private CalciteConnection calciteConn;

    private CalciteSchema() throws Exception {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("org.apache.calcite.jdbc.Driver");
        basicDataSource.setUrl("jdbc:calcite:caseSensitive=false; parserFactory=org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY; lex=MYSQL");
        basicDataSource.setRollbackOnReturn(false);
        handle = Jdbi.create(basicDataSource).open();
        Connection conn = handle.getConnection();
        calciteConn = conn.unwrap(CalciteConnection.class);
        rootSchema = calciteConn.getRootSchema();
        rootSchema.add("default", new AbstractSchema());
    }

    public static CalciteSchema getCalciteSchema() throws Exception {
        if (INSTANCE == null) {
            INSTANCE = new CalciteSchema();
        }
        return INSTANCE;
    }

    public void dropSchema(String schemaName) {
        SchemaPlus tempRootSchema = calciteConn.getRootSchema();
        for (String subSchemaName : rootSchema.getSubSchemaNames()) {
            if (!subSchemaName.equals(schemaName)) {
                tempRootSchema.add(subSchemaName, rootSchema.getSubSchema(subSchemaName));
            }
        }
        rootSchema = tempRootSchema;
    }

    public Map<String, String> describeTable(String schemaName, String tableName) throws Exception {
        SchemaPlus schema = rootSchema.getSubSchema(schemaName);
        if (schema == null) {
            throw new UserException("The schema[" + schemaName + "] do not exist");
        }
        Map<String, String> tableInfoMap = Maps.newLinkedHashMap();
        Table table = schema.getTable(tableName);
        if (table == null) {
            Collection<Function> functions = schema.getFunctions(tableName);
            for (Function function : functions) {
                if (function instanceof ViewTableMacro) {
                    ViewTableMacro viewTableMacro = (ViewTableMacro) function;
                    table = viewTableMacro.apply(null);
                    break;
                }
            }
        }

        if (table == null) {
            throw new UserException("Table table[" + tableName + "] do not exist");
        }

        RelDataType relDataType = table.getRowType(calciteConn.getTypeFactory());
        List<RelDataTypeField> fields = relDataType.getFieldList();
        for (RelDataTypeField field : fields) {
            tableInfoMap.put(field.getName(), field.getType().getFullTypeString());
        }
        return tableInfoMap;
    }

    public void addJdbcSchema(String name, String jdbcUrl, String driverClassName, String username, String password) {
        DataSource dataSource = JdbcSchema.dataSource(jdbcUrl, driverClassName, username, password);
        JdbcSchema schema = JdbcSchema.create(rootSchema, name, dataSource, null, null);
        rootSchema.add(name, schema);
    }

    public void addElasticsearchSchema(String name, String host, int port, String index, String username, String password) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestClient restClient = builder.build();
        rootSchema.add(name, new ElasticsearchSchema(restClient, new ObjectMapper(), index));
    }

    public Map<String, String> getTablesInSchema(String schemaName) throws Exception {
        Map<String, String> names = Maps.newLinkedHashMap();
        SchemaPlus schema = rootSchema.getSubSchema(schemaName);
        if (schema == null) {
            throw new UserException("The schema[" + schemaName + "] do not exist");
        }
        // tables
        Set<String> tableNames = schema.getTableNames();
        // views
        Set<String> functionNames = schema.getFunctionNames();
        Set<String> viewNames = Sets.newHashSet();
        for (String functionName : functionNames) {
            Collection<Function> functions = schema.getFunctions(functionName);
            for (Function function : functions) {
                if (function instanceof ViewTableMacro) {
                    viewNames.add(functionName);
                }
            }
        }
        tableNames.forEach(tableName -> names.putIfAbsent(tableName, "TABLE"));
        viewNames.forEach(viewName -> names.putIfAbsent(viewName, "VIEW"));
        return names;
    }

    public Pair<Map<String, String>, ResultIterator<Map<String, Object>>> query(String sql) throws Exception {
        Query query = handle.createQuery(sql);
        final Map<String, String> columnNames = Maps.newLinkedHashMap();
        query.addCustomizer(new StatementCustomizer() {
            @Override
            public void afterExecution(PreparedStatement stmt, StatementContext ctx) throws SQLException {
                ResultSetMetaData metaData = stmt.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                    String columnName = metaData.getColumnName(i);
                    String columnType = metaData.getColumnTypeName(i);
                    columnNames.putIfAbsent(columnName, columnType);
                }
            }
        });
        return Pair.of(columnNames, query.mapToMap().iterator());
    }

    public void cud(String sql) throws Exception {
        Statement stmt = handle.getConnection().createStatement();
        stmt.execute(sql);
        stmt.close();
    }
}
