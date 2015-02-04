package se.parze.sdbq;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

public class DatabaseTypePostgresSql extends DatabaseTypeMySql {

    @Override
    public boolean dataSourceBelongToType(JdbcTemplate jdbcTemplate) {
        try {
            List<Map<String, Object>> result = jdbcTemplate.queryForList("SELECT version()");
            return result != null && result.size() == 1 &&
                    result.get(0).get("version").toString().startsWith("PostgreSQL");
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getCreateQueueTableSql(String queueTableName) {
        return "Create Table If Not Exists "+queueTableName+"("+
                "id Bigserial Primary Key, "+
                "item_id Bigint Not NULL, "+
                "started_at Timestamp NULL, "+
                "prio Integer)";
    }

}
