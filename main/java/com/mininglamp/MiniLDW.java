package com.mininglamp;

import com.mininglamp.common.Config;
import com.mininglamp.nio.NMysqlServer;
import com.mininglamp.service.ExecuteEnv;

public class MiniLDW {
    public static void main(String[] args) {
        start(args);
    }

    private static void start(String[] args) {
        NMysqlServer mysqlServer = new NMysqlServer(Config.query_port, ExecuteEnv.getInstance().getScheduler());
        mysqlServer.start();
    }
}
