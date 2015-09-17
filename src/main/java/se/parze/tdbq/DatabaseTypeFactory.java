package se.parze.tdbq;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class DatabaseTypeFactory {

    private static List<DatabaseType> databaseTypeList;

    static {
        databaseTypeList = new ArrayList<DatabaseType>();
        databaseTypeList.add(new DatabaseType.HSql());
        databaseTypeList.add(new DatabaseType.H2());
        databaseTypeList.add(new DatabaseType.MySql());
        databaseTypeList.add(new DatabaseType.PostgresSql());
        databaseTypeList.add(new DatabaseType.Oracle());
    }

    public static DatabaseType getDataBaseType(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (DatabaseType databaseType : databaseTypeList) {
            if (databaseType.dataSourceBelongToType(jdbcTemplate)) {
                return databaseType;
            }
        }
        //
        throw new TdbqException("Failed to find database type for given data source.");
    }
}
