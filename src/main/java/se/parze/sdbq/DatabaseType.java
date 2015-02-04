package se.parze.sdbq;

import org.springframework.jdbc.core.JdbcTemplate;

public interface DatabaseType {

    public boolean dataSourceBelongToType(JdbcTemplate jdbcTemplate);

    public String getCreateQueueTableSql(String queueTableName, int maxJsonLength);

    public String getSqlSelectForUpdate(String queueTableName);

}
