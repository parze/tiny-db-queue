package se.parze.sdbq;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

public class QueueBuilder<T> {

    private DataSource dataSource;
    private PlatformTransactionManager platformTransactionManager;
    private Integer maxJsonLength;
    private Class<T> clazzOfItem;
    private String queueName;

    public QueueBuilder setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public QueueBuilder setPlatformTransactionManager(PlatformTransactionManager platformTransactionManager) {
        this.platformTransactionManager = platformTransactionManager;
        return this;
    }

    public QueueBuilder setMaxJsonLength(Integer maxJsonLength) {
        this.maxJsonLength = maxJsonLength;
        return this;
    }

    public QueueBuilder setClassOfItem(Class<T> clazzOfItem) {
        this.clazzOfItem = clazzOfItem;
        return this;
    }

    public QueueBuilder setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public Queue<T> build() {
        if (maxJsonLength == null) {
            maxJsonLength = 128;
        }
        if (queueName == null) {
            queueName = "queue_"+clazzOfItem.getName().replace('.', '_').toLowerCase();
        }
        if (platformTransactionManager == null) {
            platformTransactionManager = new DataSourceTransactionManager(dataSource);
        }
        return new Queue<T>(dataSource, platformTransactionManager, maxJsonLength, clazzOfItem, queueName);
    }

}
