package com.mininglamp.common;

public class DdlException extends UserException {
    public DdlException(String msg) {
        super(msg);
    }

    public DdlException(String msg, Throwable e) {
        super(msg, e);
    }
}
