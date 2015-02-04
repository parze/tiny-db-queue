package se.parze.sdbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class Queue<T> {

    private Logger logger = LoggerFactory.getLogger(Queue.class);

    private PlatformTransactionManager platformTransactionManager;
    private String name;
    private DatabaseType databaseType;
    private JdbcTemplate jdbcTemplate;

    public Queue(DataSource dataSource, String name) {
        this(dataSource, new DataSourceTransactionManager(dataSource), name);
    }

    public Queue(DataSource dataSource, PlatformTransactionManager platformTransactionManager, String name) {
        this.platformTransactionManager = platformTransactionManager;
        this.name = name;
        this.databaseType = DatabaseTypeFactory.getDataBaseType(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        createTableIfNotExists();
    }

    private String getQueueTableName() {
        return "queue_"+name;
    }

    private TransactionStatus createTransactionStatus() {
        TransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        return this.platformTransactionManager.getTransaction(transactionDefinition);
    }

    private void createTableIfNotExists() {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.execute(databaseType.getCreateQueueTableSql(getQueueTableName()));
        platformTransactionManager.commit(status);
    }

    public void addItem(Long itemId) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Insert Into " + getQueueTableName() + "(item_id, started_at) Values (?,?)", itemId, null);
        platformTransactionManager.commit(status);

    }

    public long getQueueSize() {
        return (Long) jdbcTemplate.queryForMap("Select count(id) as c From "+getQueueTableName()).get("c");
    }

    public LongIds getAndLockNextItem() {
        Long id = null;
        Long itemId = null;
        TransactionStatus status = createTransactionStatus();
        String sql = databaseType.getSqlSelectForUpdate(getQueueTableName());
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);
        if (results != null && results.size() > 0) {
            id = (Long) results.get(0).get("id");
            sql = "Update "+getQueueTableName()+" Set started_at=? Where id=?";
            jdbcTemplate.update(sql, new Date(), id);
            Map<String, Object> result = jdbcTemplate.queryForMap("Select item_id From "+getQueueTableName()+" Where id=?", id);
            if (result != null && result.size() > 0) {
                itemId = (Long) result.get("item_id");
            }
        }
        platformTransactionManager.commit(status);
        if (id == null || itemId == null) {
            return null;
        }
        return new LongIds(id, itemId);
    }

    public void removeItem(LongIds longIds) {
        TransactionStatus status = createTransactionStatus();
        jdbcTemplate.update("Delete From " + getQueueTableName() + " Where id=?", longIds.getId());
        platformTransactionManager.commit(status);
    }


}
