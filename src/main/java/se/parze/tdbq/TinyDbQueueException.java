package se.parze.tdbq;

public class TinyDbQueueException extends RuntimeException {

    public TinyDbQueueException(String msg) {
        super(msg);
    }

    public TinyDbQueueException(String msg, Exception e) {
        super(msg, e);
    }
}
