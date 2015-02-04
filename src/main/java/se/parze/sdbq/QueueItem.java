package se.parze.sdbq;

public class QueueItem<T> {

    private final Long id;
    private final T item;

    public QueueItem(Long id, T item) {
        this.id = id;
        this.item = item;
    }

    public Long getId() {
        return id;
    }

    public T getItem() {
        return item;
    }
}
