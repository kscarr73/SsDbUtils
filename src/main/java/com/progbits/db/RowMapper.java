package com.progbits.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @param <T> The Class Type for this instance
 * @author scarr
 */
public interface RowMapper<T> {
	String getSql();
    
	List<T> getList();
	
    int mapRow(ResultSet rs, int rowNum) throws SQLException;
}
