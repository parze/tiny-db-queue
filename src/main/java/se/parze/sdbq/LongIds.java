package se.parze.sdbq;

public class LongIds {

    private final Long id;
    private final Long itemId;

    public LongIds(Long id, Long itemId) {
        this.id = id;
        this.itemId = itemId;
    }

    public Long getId() {
        return id;
    }

    public Long getItemId() {
        return itemId;
    }
}
