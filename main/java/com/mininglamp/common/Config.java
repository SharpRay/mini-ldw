package com.mininglamp.common;

public class Config extends ConfigBase {

    @ConfField(mutable = true)
    public static int table_name_length_limit = 64;

    /**
     * Maximal number of thread in connection-scheduler-pool.
     */
    @ConfField public static int max_connection_scheduler_threads_num = 4096;

    /**
     * if set to false, auth check will be disable, in case some goes wrong with the new privilege system.
     */
    @ConfField public static boolean enable_auth_check = true;

    @ConfField public static boolean proxy_auth_enable = false;
    @ConfField public static String proxy_auth_magic_prefix = "x@8";

    @ConfField public static int query_port = 9031;

    /**
     * max num of thread to handle task in mysql.
     */
    @ConfField public static int max_mysql_service_task_threads_num = 4096;

    /**
     * num of thread to handle io events in mysql.
     */
    @ConfField public static int mysql_service_io_threads_num = 4;

    /**
     * The backlog_num for mysql nio server
     * When you enlarge this backlog_num, you should enlarge the value in
     * the linux /proc/sys/net/core/somaxconn file at the same time
     */
    @ConfField public static int mysql_nio_backlog_num = 1024;

    /**
     * Maximal number of connections per LDW instance.
     */
    @ConfField public static int qe_max_connection = 1024;
}
