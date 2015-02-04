package se.parze.sdbq;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class DatabaseTypeFactory {

    private static List<DatabaseType> databaseTypeList;

    static {
        databaseTypeList = new ArrayList<DatabaseType>();
        databaseTypeList.add(new DatabaseTypeHSql());
        databaseTypeList.add(new DatabaseTypeH2());
        databaseTypeList.add(new DatabaseTypeMySql());
        databaseTypeList.add(new DatabaseTypePostgresSql());
    }

    public static DatabaseType getDataBaseType(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (DatabaseType databaseType : databaseTypeList) {
            if (databaseType.dataSourceBelongToType(jdbcTemplate)) {
                return databaseType;
            }
        }
        //
        throw new SimpleDbQueueException("Failed to find database type for data source.");
    }

}
