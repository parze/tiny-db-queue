package com.github.parze;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;


public class H2QueueTest {

    private Logger logger = LoggerFactory.getLogger(H2QueueTest.class);

    private static DataSource dataSource;

    @BeforeClass
    public static void setupBeforeClass() {
        dataSource = new EmbeddedDatabaseBuilder().
                setType(EmbeddedDatabaseType.H2).
                build();
    }

    @Test
    public void testQueueExecutor() throws InterruptedException {
        final List<Long> numbers = Collections.synchronizedList(new ArrayList<Long>());
        Queue<Long> queue = new RelationalDatabaseQueue.Builder<Long>()
                .withClassOfItem(Long.class)
                .withDataSource(dataSource)
                .build();
        QueueExecutor<Long> queueExecutor = new QueueExecutor.Builder<Long>()
                .withQueue(queue)
                .withRunnableCreator(new QueueExecutor.RunnableCreator<Long>() {
                    public Runnable createRunnable(final QueueItem<Long> queueItem, final QueueExecutor.CallBackWhenDone<Long> callBackWhenDone) {
                        return new Runnable() {
                            public void run() {
                                logger.info("Adding the number " + queueItem.getItem());
                                numbers.add(queueItem.getItem());
                                callBackWhenDone.done(queueItem);
                            }
                        };
                    }
                }).build();
        // adding work
        for (int i = 0; i < 100; i++) {
            queueExecutor.addItem(100L);
        }
        // waiting to complete and shutdown
        for (int i = 0; i < 100; i++) {
            if (numbers.size() == 100) {
                break;
            }
            Thread.sleep(10);
        }
        queueExecutor.shutdown();
        // assert
        assertThat(numbers.size()).isEqualTo(100);
        for (Long number : numbers) {
            assertThat(number).isEqualTo(100L);
        }

    }

    @Test
    public void testQueuePojo() throws Exception {
        Queue<MyPojo> queue = new RelationalDatabaseQueue.Builder<MyPojo>()
                .withDataSource(dataSource)
                .withClassOfItem(MyPojo.class)
                .build();
        assertThat(queue.getQueueSize()).isEqualTo(0);
        //
        queue.addItem(new MyPojo(1, "one"));
        assertThat(queue.getQueueSize()).isEqualTo(1);
        MyPojo myPojo = queue.getAndLockNextItem().getItem();
        assertThat(myPojo.getId()).isEqualTo(1);
        assertThat(myPojo.getName()).isEqualTo("one");
    }

    @Test
    public void testQueueLong() throws Exception {
        Queue<Long> queue = new RelationalDatabaseQueue.Builder<Long>()
                .withDataSource(dataSource)
                .withClassOfItem(Long.class)
                .build();

        assertThat(queue.getQueueSize()).isEqualTo(0);
        queue.addItem(10L);
        assertThat(queue.getQueueSize()).isEqualTo(1);
        queue.addItem(20L);
        assertThat(queue.getQueueSize()).isEqualTo(2);
        //
        QueueItem queueItem0 = queue.getAndLockNextItem();
        assertThat(queueItem0).isNotNull();
        assertThat(queueItem0.getItem()).isEqualTo(10L);
        //
        QueueItem queueItem1 = queue.getAndLockNextItem();
        assertThat(queueItem1).isNotNull();
        assertThat(queueItem1.getItem()).isEqualTo(20L);
        //
        QueueItem queueItem2 = queue.getAndLockNextItem();
        assertThat(queueItem2).isNull();
        //
        assertThat(queue.getQueueSize()).isEqualTo(2);
        queue.removeItem(queueItem1);
        assertThat(queue.getQueueSize()).isEqualTo(1);
        queue.removeItem(queueItem0);
        assertThat(queue.getQueueSize()).isEqualTo(0);

    }

    @Test
    public void testQueueMultiThreaded() throws Exception {
        Queue<Long> queue = new RelationalDatabaseQueue.Builder<Long>()
                .withDataSource(dataSource)
                .withClassOfItem(Long.class)
                .build();
        assertThat(queue.getQueueSize()).isEqualTo(0);

        //
        Workers<TestWorker> workers = new Workers<TestWorker>();
        for (int i = 0; i < 10; i++) {
            workers.addWorker(new TestWorker("Worker_" + i, queue));
        }
        workers.startWorkers();

        //
        int totalItemCount = 100;
        for (int i = 0; i < totalItemCount/2; i++) {
            queue.addItem(new Long(i+50));
        }
        workers.notifyWorkersThatWorkIsReadyForProcessing();
        for (int i = 0; i < totalItemCount/2; i++) {
            queue.addItem(new Long(i+50));
        }
        workers.notifyWorkersThatWorkIsReadyForProcessing();
        workers.waitUntilAllWorkersAreDone();

        //
        int totalWorkCount = 0;
        for (TestWorker testWorker : workers.getWorkers()) {
            totalWorkCount += testWorker.getWorkCount();
            testWorker.stopWorker();
        }
        assertThat(totalWorkCount).isEqualTo(totalItemCount);

    }

    public static class MyPojo {
        private int id;
        private String name;
        public MyPojo() {

        }

        public MyPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

}