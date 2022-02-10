package org.rzlabs.qe;


public class QueryDetail {
    public enum QueryMemState {
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    };

    // When query received, FE will construct a QueryDetail
    // object. This object will set queryId, startTime, sql
    // fields. As well state is be set as RUNNING.
    // After query finished, endTime and latency will
    // be set and state will be updated to be FINISHED/FAILED/CANCELLED
    // according to the query execution results.
    // So, one query will be inserted into as a item and
    // be updated upon finished. To indicate the two event,
    // a extra field named eventTime is added.
    private long eventTime;
    private String queryId;
    private long startTime;
    // endTime and latency are update upon query finished.
    // default value will set to be minus one(-1).
    private long endTime;
    private long latency;
    private QueryMemState state;
    private String database;
    private String sql;

    public QueryDetail(long eventTime, String queryId, long startTime,
                       long endTime, long latency, QueryMemState state,
                       String database, String sql) {
        this.eventTime = eventTime;
        this.queryId = queryId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.latency = latency;
        this.state = state;
        if (database.equals("")) {
            this.database = "";
        } else {
            String[] stringPieces = database.split(":", -1);
            this.database = stringPieces[1]; // eliminate cluster name
        }
        this.sql = sql;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public long getLatency() {
        return latency;
    }

    public void setState(QueryMemState state) {
        this.state = state;
    }

    public QueryMemState getState() {
        return state;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }
}
