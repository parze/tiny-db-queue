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
            @Override
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

    public interface CallBackWhenDone<T> {
        public void done(QueueItem<T> queueItem);
    }

    public interface RunnableCreator<T> {
        public Runnable createRunnable(QueueItem<T> queueItem, CallBackWhenDone<T> callBackWhenDone);
    }


    public static class Builder<T> {

        private DataSource dataSource;
        private PlatformTransactionManager platformTransactionManager;
        private Integer maxJsonLength;
        private Class<T> clazzOfItem;
        private String queueName;

        private RunnableCreator<T> runnableCreator;
        private Long checkQueueInterval;
        private Integer threadPoolSize;

        public Builder setDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder setPlatformTransactionManager(PlatformTransactionManager platformTransactionManager) {
            this.platformTransactionManager = platformTransactionManager;
            return this;
        }

        public Builder setMaxJsonLength(Integer maxJsonLength) {
            this.maxJsonLength = maxJsonLength;
            return this;
        }

        public Builder setClassOfItem(Class<T> clazzOfItem) {
            this.clazzOfItem = clazzOfItem;
            return this;
        }

        public Builder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder setRunnableCreator(RunnableCreator<T> runnableCreator) {
            this.runnableCreator = runnableCreator;
            return this;
        }

        public Builder setCheckQueueInterval(long checkQueueInterval) {
            this.checkQueueInterval = checkQueueInterval;
            return this;
        }

        public Builder setThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public QueueExecutor<T> build() {
            //
            if (maxJsonLength == null) {
                maxJsonLength = 128;
            }
            if (queueName == null) {
                queueName = "queue_"+clazzOfItem.getName().replace('.', '_').toLowerCase();
            }
            if (platformTransactionManager == null) {
                platformTransactionManager = new DataSourceTransactionManager(dataSource);
            }
            Queue<T> queue = new Queue<T>(dataSource, platformTransactionManager, maxJsonLength,  clazzOfItem, queueName);
            //
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
