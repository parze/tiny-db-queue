package se.parze.tdbq;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class Queue<T> {

    private Logger logger = LoggerFactory.getLogger(Queue.class);

    private PlatformTransactionManager platformTransactionManager;
    private String queueName;
    private int maxJsonLength;
    private Class<T> clazzOfItem;
    private DatabaseType databaseType;
    private JdbcTemplate jdbcTemplate;
    private ObjectMapper mapper;

    protected Queue(DataSource dataSource, PlatformTransactionManager platformTransactionManager, int maxJsonLength,  Class<T> clazzOfItem, String queueName) {
        this.mapper = new ObjectMapper();
        this.maxJsonLength = maxJsonLength;
        this.clazzOfItem = clazzOfItem;
        this.platformTransactionManager = platformTransactionManager;
        this.queueName = queueName;
        this.databaseType = DatabaseTypeFactory.getDataBaseType(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        createTableIfNotExists();
    }

    private String getQueueTableName() {
        return queueName;
    }

    private TransactionStatus createTransactionStatus() {
        TransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        return this.platformTransactionManager.getTransaction(transactionDefinition);
    }

    private void createTableIfNotExists() {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.execute(databaseType.getCreateQueueTableSql(getQueueTableName(), this.maxJsonLength));
        platformTransactionManager.commit(status);
    }

    public void addItem(T item) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Insert Into " + getQueueTableName() + "(item, started_at) Values (?,?)", toJson(item), null);
        platformTransactionManager.commit(status);
    }

    private String toJson(T item) {
        try {
            return mapper.writeValueAsString(item);
        } catch (IOException e) {
            throw new TinyDbQueueException("Failed to parse item to Json.", e);
        }
    }

    private T fromJson(String str) {
        try {
            return mapper.readValue(str, this.clazzOfItem);
        } catch (IOException e) {
            throw new TinyDbQueueException("Failed to parse item to Json.", e);
        }
    }

    public long getQueueSize() {
        return (Long) jdbcTemplate.queryForMap("Select count(id) as c From "+getQueueTableName()).get("c");
    }

    public QueueItem<T> getAndLockNextItem() {
        Long id = null;
        String itemStr = null;
        TransactionStatus status = createTransactionStatus();
        String sql = databaseType.getSqlSelectForUpdate(getQueueTableName());
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);
        if (results != null && results.size() > 0) {
            id = (Long) results.get(0).get("id");
            sql = "Update "+getQueueTableName()+" Set started_at=? Where id=?";
            jdbcTemplate.update(sql, new Date(), id);
            Map<String, Object> result = jdbcTemplate.queryForMap("Select item From "+getQueueTableName()+" Where id=?", id);
            if (result != null && result.size() > 0) {
                itemStr = (String) result.get("item");
            }
        }
        platformTransactionManager.commit(status);
        if (id == null || itemStr == null) {
            return null;
        }
        return new QueueItem<T>(id, fromJson(itemStr));
    }

    public void removeItem(QueueItem<T> queueItem) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Delete From " + getQueueTableName() + " Where id=?", queueItem.getId());
        platformTransactionManager.commit(status);
    }


}
