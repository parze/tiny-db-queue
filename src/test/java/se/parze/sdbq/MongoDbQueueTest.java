package se.parze.sdbq;

import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import se.parze.tdbq.MongoDbQueue;
import se.parze.tdbq.Workers;

import static org.fest.assertions.Assertions.assertThat;

@Ignore // ignore since it is a integration test
public class MongoDbQueueTest {

    private MongoClient mongoClient;

    @Before
    public void setup() {
        this.mongoClient = new MongoClient( "localhost" , 27017);
    }

    @Test
    public void testQueueMultiThreaded() throws Exception {
        MongoDbQueue<Long> queue = new MongoDbQueue.Builder<Long>()
                .withClassOfItem(Long.class)
                .withQueueName("queue_test_long")
                .withMongoClient(this.mongoClient)
                .withDatabaseName("queue_test")
                .build();
        queue.getQueueCollection().drop();
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

}
