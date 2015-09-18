package com.github.parze;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorker  extends Worker {
    private Logger logger = LoggerFactory.getLogger(TestWorker.class);
    private int workCount = 0;
    private Queue queue;
    public TestWorker(String name, Queue queue) {
        super(name);
        this.queue = queue;
    }
    @Override
    public void computeWork() {
        QueueItem queueItem = queue.getAndLockNextItem();
        while (queueItem != null) {
            workCount++;
            logger.debug(getName() + ": (" + queueItem.getId() + " : " + queueItem.getItem() + ")");
            queue.removeItem(queueItem);
            queueItem = queue.getAndLockNextItem();
        }
    }

    public int getWorkCount() {
        return workCount;
    }
}