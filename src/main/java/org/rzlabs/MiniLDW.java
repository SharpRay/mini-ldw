package org.rzlabs;

import org.rzlabs.common.Config;
import org.rzlabs.nio.NMysqlServer;
import org.rzlabs.service.ExecuteEnv;

public class MiniLDW {
    public static void main(String[] args) {
        start(args);
    }

    private static void start(String[] args) {
        NMysqlServer mysqlServer = new NMysqlServer(Config.query_port, ExecuteEnv.getInstance().getScheduler());
        mysqlServer.start();
    }
}
