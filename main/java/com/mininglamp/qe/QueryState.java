package com.mininglamp.qe;

import com.mininglamp.common.ErrorCode;
import com.mininglamp.mysql.MysqlEofPacket;
import com.mininglamp.mysql.MysqlErrPacket;
import com.mininglamp.mysql.MysqlOkPacket;
import com.mininglamp.mysql.MysqlPacket;

/**
 * QueryState used to record state of query.
 */
public class QueryState {
    public enum MysqlStateType {
        NOOP,   // send nothing to remote
        OK,     // send OK packet to remote
        EOF,    // send EOF packet to remote
        ERR     // send ERROR packet to remote
    }

    public enum ErrType {
        ANALYSIS_ERR,
        OTHER_ERR
    }

    private MysqlStateType stateType = MysqlStateType.OK;
    private String errorMessage = "";
    private ErrorCode errorCode;
    private String infoMessage;
    private ErrType errType = ErrType.OTHER_ERR;
    private boolean isQuery = false;
    private long affectedRows = 0;
    private int warningRows = 0;
    // make it public for easy to use
    public int serverStatus = 0;

    public QueryState() {
    }

    public void reset() {
        stateType = MysqlStateType.OK;
        errorCode = null;
        infoMessage = null;
        serverStatus = 0;
        isQuery = false;
        affectedRows = 0;
        warningRows = 0;
    }

    public MysqlStateType getStateType() {
        return stateType;
    }

    public void setEof() {
        stateType = MysqlStateType.EOF;
    }

    public void setOk() {
        if (stateType == MysqlStateType.OK) {
            return;
        }
        setOk(0, 0, null);
    }

    public void setOk(long affectedRows, int warningRows, String infoMessage) {
        this.affectedRows = affectedRows;
        this.warningRows = warningRows;
        this.infoMessage = infoMessage;
        stateType = MysqlStateType.OK;
    }

    public void setError(String errorMsg) {
        this.stateType = MysqlStateType.ERR;
        this.errorMessage = errorMsg;
    }

    public void setError(ErrorCode code, String msg) {
        this.stateType = MysqlStateType.ERR;
        this.errorCode = code;
        this.errorMessage = msg;
    }

    public void setMsg(String msg) {
        this.errorMessage = msg;
    }

    public void setErrType(ErrType errType) {
        this.errType = errType;
    }

    public ErrType getErrType() {
        return errType;
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public int getWarningRows() {
        return warningRows;
    }

    public MysqlPacket toResponsePacket() {
        MysqlPacket packet = null;
        switch (stateType) {
            case OK:
                packet = new MysqlOkPacket(this);
                break;
            case EOF:
                packet = new MysqlEofPacket(this);
                break;
            case ERR:
                packet = new MysqlErrPacket(this);
                break;
            default:
                break;
        }
        return packet;
    }

    @Override
    public String toString() {
        return String.valueOf(stateType);
    }
}
