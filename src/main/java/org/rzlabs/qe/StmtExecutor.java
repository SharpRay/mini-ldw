package org.rzlabs.qe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.rzlabs.analysis.AddSchemaStmt;
import org.rzlabs.analysis.CalciteStmt;
import org.rzlabs.analysis.DescribeTableStmt;
import org.rzlabs.analysis.DropSchemaStmt;
import org.rzlabs.analysis.ParseNode;
import org.rzlabs.analysis.ShowSchemaStmt;
import org.rzlabs.analysis.ShowTableStmt;
import org.rzlabs.analysis.UseStmt;
import org.rzlabs.calcite.CalciteSchema;
import org.rzlabs.catalog.Catalog;
import org.rzlabs.catalog.Column;
import org.rzlabs.catalog.PrimitiveType;
import org.rzlabs.common.AnalysisException;
import org.rzlabs.common.UserException;
import org.rzlabs.mysql.MysqlChannel;
import org.rzlabs.mysql.MysqlEofPacket;
import org.rzlabs.mysql.MysqlSerializer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.Conversion;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdbi.v3.core.result.ResultIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    private static final int MAX_DATA_TO_SEND_FOR_TXN = 100;
    public static String null_string = "\\N";

    private ConnectContext context;
    private MysqlSerializer serializer;
    private String originStatement;
    private ParseNode stmt;

    private ShowResultSet showResultSet;

    public StmtExecutor(ConnectContext context, ParseNode parsedStmt) {
        this.context = context;
        this.stmt = parsedStmt;
        this.serializer = context.getSerializer();
    }

    public void execute() throws Exception {
        context.setStartTime();
        stmt.analyze();
        if (stmt instanceof ShowSchemaStmt) {
            handleShowSchema();
        } else if (stmt instanceof UseStmt) {
            handleUseStmt();
        } else if (stmt instanceof CalciteStmt) {
            handleCalciteStmt();
        } else if (stmt instanceof AddSchemaStmt) {
            handleAddSchema();
        } else if (stmt instanceof ShowTableStmt) {
            handleShowTable();
        } else if (stmt instanceof DescribeTableStmt) {
            handleDescribeTable();
        } else if (stmt instanceof DropSchemaStmt) {
            handleDropSchema();
        }
    }

    public ShowResultSet getShowResultSet() {
        return showResultSet;
    }

    private void handleDropSchema() throws Exception {
        DropSchemaStmt dropSchemaStmt = (DropSchemaStmt) stmt;
        String schemaName = dropSchemaStmt.getSchemaName();
        CalciteSchema.getCalciteSchema().dropSchema(schemaName);
        Catalog.currentCatalog().dropSchema(schemaName);
        context.setDatabase("default");
    }

    private void handleDescribeTable() throws Exception {
        DescribeTableStmt describeTableStmt = (DescribeTableStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String dbName = context.getDatabase();
        describeTableStmt.setDb(dbName);
        Map<String, String> tableInfoMap = CalciteSchema.getCalciteSchema().describeTable(dbName,
                describeTableStmt.getTableName());
        for (Map.Entry<String, String> entry : tableInfoMap.entrySet()) {
            List<String> row = Lists.newArrayList();
            String fieldName = entry.getKey();
            String fieldType = entry.getValue().toUpperCase();
            row.add(fieldName);
            row.add(fieldType);
            rows.add(row);
        }
        showResultSet = new ShowResultSet(describeTableStmt.getMetaData(), rows);
    }

    private void handleAddSchema() throws Exception {
        AddSchemaStmt addSchemaStmt = (AddSchemaStmt) stmt;
        addSchemaStmt.addSchema();
    }

    private void handleShowTable() throws Exception {
        ShowTableStmt showTableStmt = (ShowTableStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String dbName = context.getDatabase();
        showTableStmt.setDb(dbName);
        Map<String, String> tables = CalciteSchema.getCalciteSchema().getTablesInSchema(dbName);
        for (Map.Entry<String, String> entry : tables.entrySet()) {
            List<String> row = Lists.newArrayList();
            String tableName = entry.getKey();
            String tableType = entry.getValue().toUpperCase();
            row.add(tableName);
            row.add(tableType);
            rows.add(row);
        }
        showResultSet = new ShowResultSet(showTableStmt.getMetaData(), rows);
    }

    private void handleCalciteStmt() throws Exception {
        CalciteStmt calciteStmt = (CalciteStmt) stmt;
        SqlNode sqlNode = calciteStmt.getSqlNode();
        SqlKind kind = sqlNode.getKind();
        if (kind == SqlKind.SELECT) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            if (sqlSelect.getSelectList().size() == 1 &&
                    sqlSelect.getSelectList().get(0) instanceof SqlBasicCall &&
                    ((SqlBasicCall) sqlSelect.getSelectList().get(0)).getOperator().getName().equalsIgnoreCase("database")) {
                // SELECT DATABASE()
//                byte[] bytes = new byte[]{15, 0, 1, 11, 0, 0, 0, 1, 0, 0, 0, 10, 9, 104, 116, 97, 112, 95, 116, 101,
//                        115, 116, 2, 0, 2, 0, 10, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0};
//                ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
//                byteBuffer.put(bytes);
//                MysqlChannel channel = context.getMysqlChannel();
//                //channel.sendOnePacket(byteBuffer);
//                context.updateReturnRows(1);
                return;
            }
            queryAndWriteChannel(sqlSelect.toString());
        } else if (kind == SqlKind.ORDER_BY || kind == SqlKind.WITH || kind == SqlKind.JOIN ||
                kind == SqlKind.UNION || kind == SqlKind.INTERSECT || kind == SqlKind.EXCEPT || kind == SqlKind.MINUS) {
            queryAndWriteChannel(sqlNode.toString());
        } else if (kind == SqlKind.CREATE_TABLE || kind == SqlKind.DROP_TABLE) {
            String qualifiedSql = generateQualifiedSql(sqlNode.toString(), "TABLE");
            qualifiedSql = generateQualifiedSql(qualifiedSql, "FROM");
            CalciteSchema.getCalciteSchema().cud(qualifiedSql);
        } else if (kind == SqlKind.CREATE_VIEW || kind == SqlKind.DROP_VIEW) {
            String qualifiedSql = generateQualifiedSql(sqlNode.toString(), "VIEW");
            qualifiedSql = generateQualifiedSql(qualifiedSql, "FROM");
            CalciteSchema.getCalciteSchema().cud(qualifiedSql);
        } else if (kind == SqlKind.UPDATE) {
            String qualifiedSql = generateQualifiedSql(sqlNode.toString(), "UPDATE");
            qualifiedSql = generateQualifiedSql(qualifiedSql, "FROM");
            CalciteSchema.getCalciteSchema().cud(qualifiedSql);
        } else if (kind == SqlKind.DELETE) {
            String qualifiedSql = generateQualifiedSql(sqlNode.toString(), "FROM");
            CalciteSchema.getCalciteSchema().cud(qualifiedSql);
        } else if (kind == SqlKind.INSERT) {
            String qualifiedSql = generateQualifiedSql(sqlNode.toString(), "INTO");
            qualifiedSql = generateQualifiedSql(qualifiedSql, "FROM");
            CalciteSchema.getCalciteSchema().cud(qualifiedSql);
        } else {
            throw new UserException("The operation is not supported yet.");
        }
    }

    private void queryAndWriteChannel(String sql) throws Exception {
        String qualifiedSql = generateQualifiedSql(sql, "FROM");
        Pair<Map<String, String>, ResultIterator<Map<String, Object>>> pair = CalciteSchema.getCalciteSchema().query(qualifiedSql);
        Map<String, String> resultInfo = pair.getLeft();
        ResultIterator<Map<String, Object>> resultIter = pair.getRight();

        sendFields(new ArrayList<>(resultInfo.keySet()),
                resultInfo.values().stream().map(typeName -> PrimitiveType.fromDescription(typeName)).collect(Collectors.toList()));

        while (resultIter.hasNext()) {
            Map<String, Object> row = resultIter.next();
            writeSelectResultToChannel(context.getMysqlChannel(), row);
        }
        context.getState().setEof();

    }

    // TODO: too ugly, use Calcite in the future.
    private String generateQualifiedSql(String input, String prefix) {
        String pattern = "(" + prefix + "\\s*`[\\S]*`)";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(input);

        Set<String> toBeSubs = Sets.newHashSet();
        while (m.find()) {
            String item = m.group(0);
            String tableName = item.split(prefix)[1].trim();
            if (Pattern.compile("(`((?!\\.).)*`.`((?!\\.).)*`)").matcher(tableName).find()) {
                // `test`.`tbl`
            } else {
                // `tbl`
                tableName = tableName.substring(1, tableName.length() - 1);
                toBeSubs.add(tableName);
            }
        }

        String output = input;
        for (String tableName : toBeSubs) {
            output = output.replaceAll(String.format("%s\\s*`%s`", prefix, tableName),
                    String.format("%s `%s`.`%s`", prefix, context.getDatabase(), tableName));
        }
        return output;
    }

    private void sendFields(List<String> colNames, List<PrimitiveType> types) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            serializer.writeField(colNames.get(i), types.get(i));
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    private void writeSelectResultToChannel(MysqlChannel channel, Map<String, Object> row) throws Exception {
        List<byte[]> byteArrayList = Lists.newArrayList();
        int size = 0;
        byte nullIndicatorByte = 1;
        for (Map.Entry<String, Object> col : row.entrySet()) {
            LOG.info(col.getKey() + "\t= " + col.getValue());
            byte[] bytes;
            if (col.getValue() == null) {
                bytes = new byte[]{-5};
                nullIndicatorByte = 2;
            } else {
                bytes = String.valueOf(col.getValue()).getBytes(StandardCharsets.UTF_8);
            }

            size += bytes.length;
            if (bytes.length <= 255) {
                if (bytes.length != 1 || bytes[0] != -5) {
                    size += 1;
                }
            } else {
                size += 3;
            }
            byteArrayList.add(bytes);
        }
        byte[] totalSizeBytes = new byte[2];
        Conversion.intToByteArray(size, 0, totalSizeBytes, 0, 2);
        int totalSize = 10 + 2 + size + 16;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        byte[] prefixBytes = new byte[]{15, 0, 1, 11, 0, 0, 0, nullIndicatorByte, 0, 0};
        byte[] postfixBytes = new byte[]{2, 0, 2, 0, 10, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        byteBuffer.put(prefixBytes);
        byteBuffer.put(totalSizeBytes[1]);
        byteBuffer.put(totalSizeBytes[0]);
        for (byte[] byteArray : byteArrayList) {
            if (byteArray.length <= 255) {
                if (byteArray.length != 1 || byteArray[0] != -5) {
                    byteBuffer.put((byte) byteArray.length);
                }
            } else {
                byteBuffer.put((byte) -4);
                byte[] sizeBytes = new byte[2];
                Conversion.intToByteArray(byteArray.length, 0, sizeBytes, 0, 2);
                byteBuffer.put(sizeBytes);
            }
            byteBuffer.put(byteArray);
        }
        byteBuffer.put(postfixBytes);
        byteBuffer.position(10 + 2);
        byteBuffer.limit(10 + 2 + size);
        channel.sendOnePacket(byteBuffer);
        context.updateReturnRows(1);
    }

    private void handleUseStmt() {
        UseStmt useStmt = (UseStmt) stmt;
        try {
            context.setDatabase(useStmt.getDatabase());
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void sendMetaData(ResultSetMetaData metaData) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (Column col : metaData.getColumns()) {
            serializer.reset();
            // TODO(zhaochun): only support varchar type
            serializer.writeField(col.getName(), col.getType().getPrimitiveType());
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    public void sendResult(ResultSet resultSet) throws IOException {
        context.updateReturnRows(resultSet.getResultRows().size());
        // Send meta data.
        sendMetaData(resultSet.getMetaData());

        // Send result set.
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            for (String item : row) {
                if (item == null || item.equals(null_string)) {
                    serializer.writeNull();
                } else {
                    serializer.writeLenEncodedString(item);
                }
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }

        context.getState().setEof();
    }

    private void handleShowSchema() throws AnalysisException {
        ShowSchemaStmt showSchemaStmt = (ShowSchemaStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Set<String> schemaNameSet = Catalog.currentCatalog().schemaNames();
        for (String schemaName : schemaNameSet) {
            rows.add(Collections.singletonList(schemaName));
        }

        showResultSet = new ShowResultSet(showSchemaStmt.getMetaData(), rows);
    }
}
