package com.mininglamp.nio;


import com.mininglamp.common.Config;
import com.mininglamp.common.ThreadPoolManager;
import com.mininglamp.mysql.MysqlServer;
import com.mininglamp.qe.ConnectScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

/**
 * mysql protocol implementation based on nio.
 */
public class NMysqlServer extends MysqlServer {
    private final Logger LOG = LogManager.getLogger(this.getClass());

    private XnioWorker xnioWorker;

    private AcceptListener acceptListener;

    private AcceptingChannel<StreamConnection> server;

    // default task service.
    private ExecutorService taskService = ThreadPoolManager.newDaemonCacheThreadPool(Config.max_mysql_service_task_threads_num, "doris-mysql-nio-pool", true);

    public NMysqlServer(int port, ConnectScheduler connectScheduler) {
        this.port = port;
        this.xnioWorker = Xnio.getInstance().createWorkerBuilder()
                .setWorkerName("doris-mysql-nio")
                .setWorkerIoThreads(Config.mysql_service_io_threads_num)
                .setExternalExecutorService(taskService).build();
        // connectScheduler only used for idle check.
        this.acceptListener = new AcceptListener(connectScheduler);
    }

    // start MySQL protocol service
    // return true if success, otherwise false
    @Override
    public boolean start() {
        try {
            server = xnioWorker.createStreamConnectionServer(new InetSocketAddress(port),
                    acceptListener, OptionMap.create(Options.TCP_NODELAY, true, Options.BACKLOG, Config.mysql_nio_backlog_num));
            server.resumeAccepts();
            running = true;
            LOG.info("Open mysql server success on {}", port);
            return true;
        } catch (IOException e) {
            LOG.warn("Open MySQL network service failed.", e);
            return false;
        }
    }

    @Override
    public void stop() {
        if (running) {
            running = false;
            // close server channel, make accept throw exception
            try {
                server.close();
            } catch (IOException e) {
                LOG.warn("close server channel failed.", e);
            }
        }
    }

    public void setTaskService(ExecutorService taskService) {
        this.taskService = taskService;
    }
}
