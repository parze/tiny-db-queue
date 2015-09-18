package com.github.parze;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class RelationalDatabaseTypeFactory {

    private static List<RelationalDatabaseType> relationalDatabaseTypeList;

    static {
        relationalDatabaseTypeList = new ArrayList<RelationalDatabaseType>();
        relationalDatabaseTypeList.add(new RelationalDatabaseType.HSql());
        relationalDatabaseTypeList.add(new RelationalDatabaseType.H2());
        relationalDatabaseTypeList.add(new RelationalDatabaseType.MySql());
        relationalDatabaseTypeList.add(new RelationalDatabaseType.PostgresSql());
        relationalDatabaseTypeList.add(new RelationalDatabaseType.Oracle());
    }

    public static RelationalDatabaseType getDataBaseType(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (RelationalDatabaseType relationalDatabaseType : relationalDatabaseTypeList) {
            if (relationalDatabaseType.dataSourceBelongToType(jdbcTemplate)) {
                return relationalDatabaseType;
            }
        }
        //
        throw new TdbqException("Failed to find database type for given data source.");
    }
}
