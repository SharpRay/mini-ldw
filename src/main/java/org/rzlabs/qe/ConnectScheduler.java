package org.rzlabs.qe;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.rzlabs.common.Config;
import org.rzlabs.common.ThreadPoolManager;
import org.rzlabs.mysql.MysqlProto;
import org.rzlabs.nio.NConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global query request scheduler.
 * Current policy is simple, one request correspond to an individual serving thread.
 */
public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private int maxConnections;
    private int numberConnection;
    private AtomicInteger nextConnectionId;
    private Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    private Map<String, AtomicInteger> connByUser = Maps.newHashMap();
    private ExecutorService executor = ThreadPoolManager.newDaemonCacheThreadPool(
            Config.max_connection_scheduler_threads_num, "Connect-Scheduler-Pool", true);

    // Use a thread toe check whether connection is timeout. Since
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough.
    private ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "Connect-Scheduler-Check-Timer", true);

    public ConnectScheduler(int maxConnections) {
        this.maxConnections = maxConnections;
        numberConnection = 0;
        nextConnectionId = new AtomicInteger(0);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    public class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            synchronized (ConnectScheduler.this) {
                for (ConnectContext connectContext : connectionMap.values()) {
                    connectContext.checkTimeout(now);
                }
            }
        }
    }

    // submit one MysqlContext to this scheduler.
    // return true, if this connection has been successfully submitted, otherwise return false.
    // Caller should close ConnectContext if return false.
    public boolean submit(ConnectContext context) {
        if (context == null) {
            return false;
        }

        context.setConnectionId(nextConnectionId.getAndAdd(1));
        // no necessary for nio.
        if(context instanceof NConnectContext){
            return true;
        }
        executor.submit(new LoopHandler(context));
        return true;
    }

    // Register one connection with its connection id.
    public synchronized boolean registerConnection(ConnectContext ctx) {
        if (numberConnection >= maxConnections) {
            return false;
        }
        // Check user
        if (connByUser.get(ctx.getQualifiedUser()) == null) {
            connByUser.put(ctx.getQualifiedUser(), new AtomicInteger(0));
        }
        int conns = connByUser.get(ctx.getQualifiedUser()).get();
        if (ctx.getIsTempUser()) {
            if (conns >= 100) {
                return false;
            }
        } else if (conns >= 100) {
            return false;
        }
        numberConnection++;
        connByUser.get(ctx.getQualifiedUser()).incrementAndGet();
        connectionMap.put((long) ctx.getConnectionId(), ctx);
        return true;
    }

    public synchronized void unregisterConnection(ConnectContext ctx) {
        if (connectionMap.remove((long) ctx.getConnectionId()) != null) {
            numberConnection--;
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
        }
    }

    public synchronized ConnectContext getContext(long connectionId) {
        return connectionMap.get(connectionId);
    }

    public synchronized int getConnectionNum() {
        return numberConnection;
    }

    public synchronized List<ConnectContext.ThreadInfo> listConnection(String user) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();

        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
//            if (!ctx.getQualifiedUser().equals(user) &&
//                    !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
//                            PrivPredicate.GRANT)) {
//                continue;
//            }

            infos.add(ctx.toThreadInfo());
        }
        return infos;
    }

    private class LoopHandler implements Runnable {
        ConnectContext context;

        LoopHandler(ConnectContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                // Set thread local info
                context.setThreadLocalInfo();
                context.setConnectScheduler(ConnectScheduler.this);
                // authenticate check failed.
                if (!MysqlProto.negotiate(context)) {
                    return;
                }

                if (registerConnection(context)) {
                    MysqlProto.sendResponsePacket(context);
                } else {
                    context.getState().setError("Reach limit of connections");
                    MysqlProto.sendResponsePacket(context);
                    return;
                }

                context.setStartTime();
                ConnectProcessor processor = new ConnectProcessor(context);
                processor.loop();
            } catch (Exception e) {
                // for unauthorized access such lvs probe request, may cause exception, just log it in debug level
                if (context.getCurrentUserIdentity() != null) {
                    LOG.warn("connect processor exception because ", e);
                } else {
                    LOG.debug("connect processor exception because ", e);
                }
            } finally {
                unregisterConnection(context);
                context.cleanup();
            }
        }
    }
}
