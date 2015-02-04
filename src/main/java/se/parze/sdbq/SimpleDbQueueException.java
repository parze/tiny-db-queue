package se.parze.sdbq;

public class SimpleDbQueueException extends RuntimeException {

    public SimpleDbQueueException(String msg) {
        super(msg);
    }

    public SimpleDbQueueException(String msg, Exception e) {
        super(msg, e);
    }
}
