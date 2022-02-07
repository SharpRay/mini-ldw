package com.mininglamp.common;

import com.mininglamp.qe.ConnectContext;

public class ErrorReport {

    private static String reportCommon(String pattern, ErrorCode errorCode, Object... objs) {
        String errMsg;
        if (pattern == null) {
            errMsg = errorCode.formatErrorMsg(objs);
        } else {
            errMsg = String.format(pattern, objs);
        }
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            ctx.getState().setError(errorCode, errMsg);
        }
        // TODO(zc): think about LOG to file
        return errMsg;
    }

    public static void reportAnalysisException(String pattern, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs));
    }

    public static void reportAnalysisException(ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        reportAnalysisException(null, errorCode, objs);
    }

    public static void reportAnalysisException(String pattern, ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, errorCode, objs));
    }

    public static void reportDdlException(String pattern, Object... objs)
            throws DdlException {
        reportDdlException(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs);
    }

    public static void reportDdlException(ErrorCode errorCode, Object... objs)
            throws DdlException {
        reportDdlException(null, errorCode, objs);
    }

    public static void reportDdlException(String pattern, ErrorCode errorCode, Object... objs)
            throws DdlException {
        throw new DdlException(reportCommon(pattern, errorCode, objs));
    }

    public static void report(String pattern, Object... objs) {
        report(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs);
    }

    public static void report(ErrorCode errorCode, Object... objs) {
        report(null, errorCode, objs);
    }

    public static void report(String pattern, ErrorCode errorCode, Object... objs) {
        reportCommon(pattern, errorCode, objs);
    }
}
