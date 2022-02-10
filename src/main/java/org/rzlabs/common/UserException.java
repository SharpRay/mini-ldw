package org.rzlabs.common;


import com.google.common.base.Strings;

/**
 * Thrown for internal server errors.
 */
public class UserException extends Exception {
    private InternalErrorCode errorCode;
    public UserException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(Throwable cause) {
        super(cause);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(Strings.nullToEmpty(msg), cause, enableSuppression, writableStackTrace);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(String msg) {
        super(Strings.nullToEmpty(msg));
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(InternalErrorCode errCode, String msg) {
        super(Strings.nullToEmpty(msg));
        this.errorCode = errCode;
    }

    public InternalErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return errorCode + ", detailMessage = " + super.getMessage();
    }
}
