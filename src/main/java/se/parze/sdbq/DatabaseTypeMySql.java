package se.parze.sdbq;

import org.springframework.jdbc.core.JdbcTemplate;

public class DatabaseTypeMySql implements DatabaseType {

    @Override
    public boolean dataSourceBelongToType(JdbcTemplate jdbcTemplate) {
        try {
            return jdbcTemplate.queryForList("SHOW VARIABLES LIKE 'version_comment'").size() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getCreateQueueTableSql(String queueTableName, int maxJsonLength) {
        return "Create Table If Not Exists "+queueTableName+"("+
                "id Bigint NOT NULL AUTO_INCREMENT, "+
                "item varchar("+maxJsonLength+") Not NULL, "+
                "started_at Timestamp NULL, "+
                "prio Integer, "+
                "PRIMARY KEY (id))";
    }

    @Override
    public String getSqlSelectForUpdate(String queueTableName) {
        return "Select id From "+queueTableName+" "+
                "Where started_at is NULL "+
                "Order By id " +
                "Limit 1 "+
                "For Update";
    }

}
