package se.parze.tdbq;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RelationalDatabaseQueue<T> extends Queue<T> {

    private Logger logger = LoggerFactory.getLogger(RelationalDatabaseQueue.class);

    private PlatformTransactionManager platformTransactionManager;
    private int maxJsonLength;
    private RelationalDatabaseType relationalDatabaseType;
    private JdbcTemplate jdbcTemplate;

    protected RelationalDatabaseQueue(DataSource dataSource, PlatformTransactionManager platformTransactionManager, int maxJsonLength,  Class<T> clazzOfItem, String queueName) {
        super(clazzOfItem, queueName);
        this.maxJsonLength = maxJsonLength;
        this.platformTransactionManager = platformTransactionManager;
        this.relationalDatabaseType = RelationalDatabaseTypeFactory.getDataBaseType(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        createTableIfNotExists();
    }

    private TransactionStatus createTransactionStatus() {
        TransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        return this.platformTransactionManager.getTransaction(transactionDefinition);
    }

    private void createTableIfNotExists() {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.execute(relationalDatabaseType.getCreateQueueTableSql(getQueueTableName(), this.maxJsonLength));
        platformTransactionManager.commit(status);
    }

    public String getQueueTableName() {
        return getQueueName();
    }

    @Override
    public void addItem(T item) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Insert Into " + getQueueTableName() + "(item, started_at) Values (?,?)", toJson(item), null);
        platformTransactionManager.commit(status);
    }

    @Override
    public long getQueueSize() {
        return (Long) jdbcTemplate.queryForMap("Select count(id) as c From "+getQueueTableName()).get("c");
    }

    @Override
    public QueueItem<T> getAndLockNextItem() {
        Long id = null;
        String itemStr = null;
        TransactionStatus status = createTransactionStatus();
        String sql = relationalDatabaseType.getSqlSelectForUpdate(getQueueTableName());
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

    @Override
    public void removeItem(QueueItem<T> queueItem) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Delete From " + getQueueTableName() + " Where id=?", queueItem.getId());
        platformTransactionManager.commit(status);
    }


    public static class Builder<T> {

        private DataSource dataSource;
        private PlatformTransactionManager platformTransactionManager;
        private Integer maxJsonLength;
        private Class<T> clazzOfItem;
        private String queueName;

        public Builder withDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder withPlatformTransactionManager(PlatformTransactionManager platformTransactionManager) {
            this.platformTransactionManager = platformTransactionManager;
            return this;
        }

        public Builder withMaxJsonLength(Integer maxJsonLength) {
            this.maxJsonLength = maxJsonLength;
            return this;
        }

        public Builder withClassOfItem(Class<T> clazzOfItem) {
            this.clazzOfItem = clazzOfItem;
            return this;
        }

        public Builder withQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Queue<T> build() {
            if (dataSource == null || clazzOfItem == null) {
                throw new TdbqException("Data source and class of item must be set.");
            }
            if (maxJsonLength == null) {
                maxJsonLength = 128;
            }
            if (queueName == null) {
                queueName = "queue_"+clazzOfItem.getName().replace('.', '_').toLowerCase();
            }
            if (platformTransactionManager == null) {
                platformTransactionManager = new DataSourceTransactionManager(dataSource);
            }
            return new RelationalDatabaseQueue<T>(dataSource, platformTransactionManager, maxJsonLength, clazzOfItem, queueName);
        }

    }



}
