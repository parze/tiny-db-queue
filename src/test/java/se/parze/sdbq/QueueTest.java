package se.parze.sdbq;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;

import static org.fest.assertions.Assertions.assertThat;

public class QueueTest {

    private Logger logger = LoggerFactory.getLogger(QueueTest.class);

    private static DataSource dataSource;

    @BeforeClass
    public static void setupBeforeClass() {
        dataSource = new EmbeddedDatabaseBuilder().
                setType(EmbeddedDatabaseType.H2).
                build();
    }

    @Test
    public void testQueuePojo() throws Exception {
        Queue<MyPojo> queue = new Queue<MyPojo>(dataSource, MyPojo.class);
        assertThat(queue.getQueueSize()).isEqualTo(0);
        queue.addItem(new MyPojo(1, "one"));
        assertThat(queue.getQueueSize()).isEqualTo(1);
        MyPojo myPojo = queue.getAndLockNextItem().getItem();
        assertThat(myPojo.getId()).isEqualTo(1);
        assertThat(myPojo.getName()).isEqualTo("one");
    }

    @Test
    public void testQueueLong() throws Exception {
        Queue<Long> queue = new Queue(dataSource, Long.class);

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
        Queue<Long> queue = new Queue<Long>(dataSource, Long.class);
        //
        Agents<TestAgent> agents = new Agents<TestAgent>();
        for (int i = 0; i < 10; i++) {
            agents.addAgent(new TestAgent("agent_"+i, queue));
        }
        agents.startAgents();

        //
        int totalItemCount = 100;
        for (int i = 0; i < totalItemCount/2; i++) {
            queue.addItem(new Long(i+50));
        }
        agents.notifyAgentsThatWorkIsReadyForProcessing();
        for (int i = 0; i < totalItemCount/2; i++) {
            queue.addItem(new Long(i+50));
        }
        agents.waitUntilAllAgentsAreDone();

        //
        int totalWorkCount = 0;
        for (TestAgent testAgent : agents.getAgents()) {
            totalWorkCount += testAgent.getWorkCount();
            testAgent.stopAgent();
        }
        assertThat(totalWorkCount).isEqualTo(totalItemCount);

    }


    public static class TestAgent extends Agent {
        private Logger logger = LoggerFactory.getLogger(TestAgent.class);
        private int workCount = 0;
        private Queue queue;
        public TestAgent(String name, Queue queue) {
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