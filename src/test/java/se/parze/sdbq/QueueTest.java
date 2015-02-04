package se.parze.sdbq;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

import javax.sql.DataSource;

import static org.fest.assertions.Assertions.assertThat;

public class QueueTest {

    private Logger logger = LoggerFactory.getLogger(QueueTest.class);

    private static DataSource dataSource;
    private Queue queue;


    @BeforeClass
    public static void setupBeforeClass() {
        dataSource = new EmbeddedDatabaseBuilder().
                setType(EmbeddedDatabaseType.H2).
                build();
    }

    @Before
    public void setup() {
        queue = new Queue(dataSource, "test");
    }

    @Test
    public void testQueue() throws Exception {

        assertThat(queue.getQueueSize()).isEqualTo(0);
        queue.addItem(10L);
        assertThat(queue.getQueueSize()).isEqualTo(1);
        queue.addItem(20L);
        assertThat(queue.getQueueSize()).isEqualTo(2);
        //
        LongIds longIds0 = queue.getAndLockNextItem();
        assertThat(longIds0).isNotNull();
        assertThat(longIds0.getItemId()).isEqualTo(10);
        //
        LongIds longIds1 = queue.getAndLockNextItem();
        assertThat(longIds1).isNotNull();
        assertThat(longIds1.getItemId()).isEqualTo(20);
        //
        LongIds longIds2 = queue.getAndLockNextItem();
        assertThat(longIds2).isNull();
        //
        assertThat(queue.getQueueSize()).isEqualTo(2);
        queue.removeItem(longIds1);
        assertThat(queue.getQueueSize()).isEqualTo(1);
        queue.removeItem(longIds0);
        assertThat(queue.getQueueSize()).isEqualTo(0);

    }

    @Test
    public void testQueueMultiThreaded() throws Exception {
        //
        Agents<TestAgent> agents = new Agents<TestAgent>();
        for (int i = 0; i < 10; i++) {
            agents.addAgent(new TestAgent("agent_"+i));
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


    public class TestAgent extends Agent {
        private int workCount = 0;
        public TestAgent(String name) {
            super(name);
        }
        @Override
        public void computeWork() {
            LongIds longIds = queue.getAndLockNextItem();
            while (longIds != null) {
                workCount++;
                logger.debug(getName() + ": (" + longIds.getId() + " : " + longIds.getItemId() + ")");
                queue.removeItem(longIds);
                longIds = queue.getAndLockNextItem();
            }
        }

        public int getWorkCount() {
            return workCount;
        }
    }

}