package com.mininglamp.qe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mininglamp.analysis.UserIdentity;
import com.mininglamp.catalog.Catalog;
import com.mininglamp.common.UserException;
import com.mininglamp.mysql.MysqlCapability;
import com.mininglamp.mysql.MysqlChannel;
import com.mininglamp.mysql.MysqlCommand;
import com.mininglamp.mysql.MysqlSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;

public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<ConnectContext>();

    // id for this connection
    protected volatile int connectionId;
    // mysql net
    protected volatile MysqlChannel mysqlChannel;
    // state
    protected volatile QueryState state;
    protected volatile long returnRows;
    // the protocol capability which server say it can support
    protected volatile MysqlCapability serverCapability;
    // the protocol capability after server and client negotiate
    protected volatile MysqlCapability capability;
    // indicates if this client is killed
    protected volatile boolean isKilled;
    // Db
    protected volatile String currentDb = "default";
    protected volatile long currentDbId = -1;

    // username@host of current login user
    protected volatile String qualifiedUser;
    // LDAP authenticated but the account LDW account does not exist, set this flag, and the user login LDW as temporary user
    protected volatile boolean isTempUser = false;
    // username@host combination for the LDW account
    // that the server used to authenticate the current client.
    // In other word, currentUserIdentity is the entry that matched in LDW auth table.
    // This account determines user's access privileges.
    protected volatile UserIdentity currentUserIdentity;
    // serializer used to pack MySQL packet
    protected volatile MysqlSerializer serializer;
    // scheduler this connection belongs to
    protected volatile ConnectScheduler connectScheduler;
    // executor
    protected volatile StmtExecutor executor;

    // command this connection is processing
    protected volatile MysqlCommand command;
    // timestamp in millisecond last command starts at
    protected volatile long startTime;
    // Cache thread info for this connection
    protected volatile ThreadInfo threadInfo;

    protected boolean isSend;

    protected String remoteIP;

    // This is used to statistic the current query details.
    // This property will only be set when the query starts to execute.
    // So in the query planning stage, do not use any value in athis attribute.
    protected QueryDetail queryDetail;


    // If set to true, the nondeterministic function will not be rewrote to constant.
    private boolean notEvalNondeterministicFunction = false;
    // If set to true, the resource tags set in resourceTags will be used to limit the query resources.
    // If set to false, the system will not restrict query resources.
    private boolean isResourceTagsSet = false;

    private String sqlHash;

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public void setIsSend(boolean isSend) {
        this.isSend = isSend;
    }

    public boolean isSend() {
        return this.isSend;
    }

    public void setNotEvalNondeterministicFunction(boolean notEvalNondeterministicFunction) {
        this.notEvalNondeterministicFunction = notEvalNondeterministicFunction;
    }

    public boolean notEvalNondeterministicFunction() {
        return notEvalNondeterministicFunction;
    }

    public ConnectContext() {
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        serializer = MysqlSerializer.newInstance();
        command = MysqlCommand.COM_SLEEP;
    }

    public ConnectContext(SocketChannel channel) {
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        mysqlChannel = new MysqlChannel(channel);
        serializer = MysqlSerializer.newInstance();
        command = MysqlCommand.COM_SLEEP;
        if (channel != null) {
            remoteIP = mysqlChannel.getRemoteIp();
        }
        queryDetail = null;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        this.queryDetail = queryDetail;
    }

    public QueryDetail getQueryDetail() {
        return queryDetail;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public long getCurrentDbId() {
        return currentDbId;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public void setQualifiedUser(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    public boolean getIsTempUser() { return isTempUser;}

    public void setIsTempUser(boolean isTempUser) { this.isTempUser = isTempUser;}

    // for USER() function
    public UserIdentity getUserIdentity() {
        return new UserIdentity(qualifiedUser, remoteIP);
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
        returnRows = 0;
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
        returnRows = 0;
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public QueryState getState() {
        return state;
    }

    public void setState(QueryState state) {
        this.state = state;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) throws UserException {
        if (!Catalog.currentCatalog().schemaNames().contains(db)) {
            throw new UserException("The schema '" + db + "' do not exist");
        }
        currentDb = db;
    }

    public void cleanup() {
        mysqlChannel.close();
        threadLocalInfo.remove();
        returnRows = 0;
    }

    public boolean isKilled() {
        return isKilled;
    }

    // Set kill flag to true;
    public void setKilled() {
        isKilled = true;
    }

    public void setExecutor(StmtExecutor executor) {
        this.executor = executor;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public void setSqlHash(String sqlHash) {
        this.sqlHash = sqlHash;
    }

    // kill operation with no protect.
//    public void kill(boolean killConnection) {
//        LOG.warn("kill timeout query, {}, kill connection: {}",
//                getMysqlChannel().getRemoteHostPortString(), killConnection);
//
//        if (killConnection) {
//            isKilled = true;
//            // Close channel to break connection with client
//            getMysqlChannel().close();
//        }
//        // Now, cancel running process.
//        StmtExecutor executorRef = executor;
//        if (executorRef != null) {
//            executorRef.cancel();
//        }
//    }

    public void checkTimeout(long now) {
        if (startTime <= 0) {
            return;
        }

        long delta = now - startTime;
        boolean killFlag = false;
        boolean killConnection = false;
        if (command == MysqlCommand.COM_SLEEP) {
            if (delta > 28800 * 1000) {
                // Need kill this connection.
                LOG.warn("kill wait timeout connection, remote: {}, wait timeout: {}",
                        getMysqlChannel().getRemoteHostPortString(), 28800);

                killFlag = true;
                killConnection = true;
            }
        } else {
            if (delta > 300 * 1000) {
                LOG.warn("kill query timeout, remote: {}, query timeout: {}",
                        getMysqlChannel().getRemoteHostPortString(), 300);

                // Only kill
                killFlag = true;
            }
        }
        if (killFlag) {
//            kill(killConnection);
        }
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo() {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        return threadInfo;
    }

    public boolean isResourceTagsSet() {
        return isResourceTagsSet;
    }

    public class ThreadInfo {
        public List<String> toRow(long nowMs) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(qualifiedUser);
            row.add(getMysqlChannel().getRemoteHostPortString());
            row.add(currentDb);
            row.add(command.toString());
            row.add("" + (nowMs - startTime) / 1000);
            row.add("");
            row.add("");
            return row;
        }
    }
}
