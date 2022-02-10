package org.rzlabs.common;

public enum InternalErrorCode {
    OK(0),

    // for common error
    IMPOSSIBLE_ERROR_ERR(1),
    INTERNAL_ERR(2),
    REPLICA_FEW_ERR(3),
    PARTITIONS_ERR(4),
    DB_ERR(5),
    TABLE_ERR(6),
    META_NOT_FOUND_ERR(7),

    // for load job error
    MANUAL_PAUSE_ERR(100),
    MANUAL_STOP_ERR(101),
    TOO_MANY_FAILURE_ROWS_ERR(102),
    CREATE_TASKS_ERR(103),
    TASKS_ABORT_ERR(104);

    private long errCode;
    private InternalErrorCode(long code) {
        this.errCode = code;
    }

    @Override
    public String toString() {
        return "errCode = " + errCode;
    }
}
