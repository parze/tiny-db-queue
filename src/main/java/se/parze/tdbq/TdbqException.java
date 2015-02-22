package se.parze.tdbq;

public class TdbqException extends RuntimeException {

    public TdbqException(String msg) {
        super(msg);
    }

    public TdbqException(String msg, Exception e) {
        super(msg, e);
    }
}
