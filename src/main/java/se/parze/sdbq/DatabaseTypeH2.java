package se.parze.sdbq;

import org.springframework.jdbc.core.JdbcTemplate;

public class DatabaseTypeH2 extends DatabaseTypeHSql {

    @Override
    public boolean dataSourceBelongToType(JdbcTemplate jdbcTemplate) {
        try {
            return jdbcTemplate.queryForList("Select * From INFORMATION_SCHEMA.SETTINGS Where name='MVCC'").size() > 0;
        } catch (Exception e) {
            return false;
        }
    }

}
