package se.parze.tdbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueueExecutor<T> {

    private Logger logger = LoggerFactory.getLogger(QueueExecutor.class);

    private final Queue<T> queue;
    private final RunnableCreator<T> runnableCreator;
    private final long checkQueueInterval;

    private final ExecutorService executorService;
    private final CallBackWhenDone<T> callBackWhenDone;
    private final Thread queueManager;
    private final Object dequeueThreadIsWorkingLock = new Object();
    private boolean shouldBeActive = true;

    public QueueExecutor(Queue<T> queue, RunnableCreator<T> runnableCreator, int threadPoolSize, final long checkQueueInterval) {
        this.queue = queue;
        this.runnableCreator = runnableCreator;
        this.checkQueueInterval = checkQueueInterval;
        //
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.callBackWhenDone = new CallBackWhenDone<T>() {
            public void done(QueueItem<T> queueItem) {
                QueueExecutor.this.queue.removeItem(queueItem);
            }
        };
        //
        this.queueManager = new Thread() {
            @Override
            public void run() {
                logger.info("Starting queue manager");
                while (shouldBeActive) {
                    synchronized (dequeueThreadIsWorkingLock) {
                        try {
                            QueueItem<T> queueItem = QueueExecutor.this.queue.getAndLockNextItem();
                            while (queueItem != null) {
                                Runnable runnable = QueueExecutor.this.runnableCreator.createRunnable(queueItem,
                                        callBackWhenDone);
                                executorService.execute(runnable);
                                queueItem = QueueExecutor.this.queue.getAndLockNextItem();
                            }
                            if (checkQueueInterval > 0) {
                                dequeueThreadIsWorkingLock.wait(checkQueueInterval);
                            } else {
                                dequeueThreadIsWorkingLock.wait();
                            }
                        } catch (InterruptedException e) {}
                    }
                }
            }
        };
        this.queueManager.start();
    }


    public void addItem(T item) {
        this.queue.addItem(item);
        notifyThatItemWasAddedToQueueForProcessing();
    }

    public void notifyThatItemWasAddedToQueueForProcessing() {
        synchronized (dequeueThreadIsWorkingLock) {
            dequeueThreadIsWorkingLock.notify();
        }
    }

    public void shutdown() {
        synchronized (dequeueThreadIsWorkingLock) {
            logger.info("Stopping queue manager ...");
            this.shouldBeActive = false;
            dequeueThreadIsWorkingLock.notify();
            this.executorService.shutdown();
            try {
                this.executorService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            logger.info("Queue manager stopped");
        }
    }

    public Queue<T> getQueue() {
        return this.queue;
    }

    public interface CallBackWhenDone<T> {
        void done(QueueItem<T> queueItem);
    }

    public interface RunnableCreator<T> {
        Runnable createRunnable(QueueItem<T> queueItem, CallBackWhenDone<T> callBackWhenDone);
    }


    public static class Builder<T> {

        private Queue<T> queue;
        private RunnableCreator<T> runnableCreator;
        private Long checkQueueInterval;
        private Integer threadPoolSize;

        public Builder withQueue(Queue<T> queue) {
            this.queue = queue;
            return this;
        }

        public Builder withRunnableCreator(RunnableCreator<T> runnableCreator) {
            this.runnableCreator = runnableCreator;
            return this;
        }

        public Builder withCheckQueueInterval(long checkQueueInterval) {
            this.checkQueueInterval = checkQueueInterval;
            return this;
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public QueueExecutor<T> build() {
            if (this.queue == null) {
                throw new TdbqException("Queue must be set.");
            }
            if (checkQueueInterval == null) {
                checkQueueInterval = -1L;
            }
            if (threadPoolSize == null) {
                threadPoolSize = 10;
            }
            return new QueueExecutor<T>(queue, this.runnableCreator, this.threadPoolSize, this.checkQueueInterval);
        }

    }

}
