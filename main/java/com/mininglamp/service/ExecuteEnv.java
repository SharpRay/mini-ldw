package com.mininglamp.service;


import com.mininglamp.common.Config;
import com.mininglamp.qe.ConnectScheduler;

// Execute environment, used to save other module, need to singleton
public class ExecuteEnv {
    private static volatile ExecuteEnv INSTANCE;
    private ConnectScheduler scheduler;

    private ExecuteEnv() {
        scheduler = new ConnectScheduler(Config.qe_max_connection);
    }

    public static ExecuteEnv getInstance() {
        if (INSTANCE == null) {
            synchronized (ExecuteEnv.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ExecuteEnv();
                }
            }
        }
        return INSTANCE;
    }

    public ConnectScheduler getScheduler() {
        return scheduler;
    }
}
