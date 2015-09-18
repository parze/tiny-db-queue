package com.github.parze;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MongoDbQueue<T> extends Queue<T> {

    private Logger logger = LoggerFactory.getLogger(MongoDbQueue.class);

    private static String FIELD_CREATED_SEQUENCE = "created_sequence";
    private static String FIELD_STARTED_AT = "started_at";
    private static String FIELD_ITEM = "item";
    private static String COLLECTION_NAME_COUNTERS = "counters";

    private MongoDatabase mongoDatabase;
    private DB mongoDb;
    private DBCollection queueCollection;
    private DBCollection queueCounterCollection;
    private String counterName;

    protected MongoDbQueue(Class<T> clazzOfItem, MongoClient mongoClient, String databaseName, String queueName) {
        super(clazzOfItem, queueName);
        this.mongoDatabase = mongoClient.getDatabase(databaseName);
        this.mongoDb = mongoClient.getDB(databaseName);
        this.queueCollection = this.mongoDb.getCollection(queueName); //mongoDatabase.getCollection(queueName);
        this.queueCounterCollection = this.mongoDb.getCollection(COLLECTION_NAME_COUNTERS);
        this.counterName = queueName+"_counter";
    }

    public DBCollection getQueueCollection() {
        return this.queueCollection;
    }

    @Override
    public void addItem(T item) {
        BasicDBObject document = new BasicDBObject()
                .append(FIELD_CREATED_SEQUENCE, getNextSequenceNumber())
                .append(FIELD_ITEM, toJson(item))
                .append(FIELD_STARTED_AT, null);
        queueCollection.insert(document);
        logger.info("Added item " + item.toString() + " to the queue.");
    }

    public Integer getNextSequenceNumber() {
        BasicDBObject query = new BasicDBObject("name", new BasicDBObject("$eq", counterName));
        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject("number", 1));
        DBObject dbObject = queueCounterCollection.findAndModify(query, null, null, false, update, true, true);
        return (Integer) dbObject.get("number");
    }

    @Override
    public long getQueueSize() {
        return queueCollection.find().count();
    }

    @Override
    public QueueItem<T> getAndLockNextItem() {
        BasicDBObject query = new BasicDBObject(FIELD_STARTED_AT, new BasicDBObject("$eq", null));
        BasicDBObject sort = new BasicDBObject(FIELD_CREATED_SEQUENCE, 1);
        String startedAt = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now());
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject(FIELD_STARTED_AT, startedAt));
        DBObject queueItem = queueCollection.findAndModify(query, null, sort, false, update, true, false);
        if (queueItem == null) {
            return null;
        }
        //
        Integer id = (Integer) queueItem.get(FIELD_CREATED_SEQUENCE);
        T item = fromJson(queueItem.get(FIELD_ITEM).toString());
        logger.info("Locked and retrieved item id:"+id+" = "+item.toString()+" from the queue.");
        return new QueueItem<T>(new Long(id), item);
    }

    @Override
    public void removeItem(QueueItem<T> queueItem) {
        BasicDBObject query = new BasicDBObject(FIELD_CREATED_SEQUENCE, queueItem.getId());
        WriteResult result = queueCollection.remove(query);
        logger.info("Removed item id:"+queueItem.getId()+" = "+queueItem.getItem().toString()+" from the queue.");
    }

    public static class Builder<T> {

        private MongoClient mongoClient;
        private String databaseName;
        private String queueName;

        private Class<T> clazzOfItem;

        public Builder withClassOfItem(Class<T> clazzOfItem) {
            this.clazzOfItem = clazzOfItem;
            return this;
        }

        public Builder withQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder<T> withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder<T> withMongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
            return this;
        }

        public MongoDbQueue<T> build() {
            if (this.databaseName == null || this.mongoClient == null || clazzOfItem == null) {
                throw new TdbqException("Mongo client, database name, and class of item must be set.");
            }
            if (queueName == null) {
                queueName = "queue_"+clazzOfItem.getName().replace('.', '_').toLowerCase();
            }
            return new MongoDbQueue<T>(clazzOfItem, mongoClient, databaseName, queueName);
        }

    }


}
