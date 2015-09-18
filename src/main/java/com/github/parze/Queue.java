package com.github.parze;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public abstract class Queue<T> {

    private Logger logger = LoggerFactory.getLogger(Queue.class);

    private String queueName;
    private Class<T> clazzOfItem;
    private ObjectMapper mapper;

    protected Queue(Class<T> clazzOfItem, String queueName) {
        this.mapper = new ObjectMapper();
        this.clazzOfItem = clazzOfItem;
        this.queueName = queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    public abstract void addItem(T item);

    protected String toJson(T item) {
        try {
            return mapper.writeValueAsString(item);
        } catch (IOException e) {
            throw new TdbqException("Failed to parse item to Json.", e);
        }
    }

    protected T fromJson(String str) {
        try {
            return mapper.readValue(str, this.clazzOfItem);
        } catch (IOException e) {
            throw new TdbqException("Failed to parse item to Json.", e);
        }
    }

    public abstract long getQueueSize();

    public abstract QueueItem<T> getAndLockNextItem();

    public abstract void removeItem(QueueItem<T> queueItem);

}
