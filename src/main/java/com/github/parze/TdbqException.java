package com.github.parze;

public class TdbqException extends RuntimeException {

    public TdbqException(String msg) {
        super(msg);
    }

    public TdbqException(String msg, Exception e) {
        super(msg, e);
    }
}
