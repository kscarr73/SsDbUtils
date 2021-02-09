/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.progbits.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author KCarr
 */
public class RunLimitTableMapper implements RowMapper<RunLimitTable> {
	private String SQL = "SELECT itmIdx, entryName "
			+ "FROM testLimitTable ORDER BY itmIdx";
	private List<RunLimitTable> _list = new ArrayList<RunLimitTable>();
	
	@Override
	public String getSql() {
		return SQL;
	}
	
	@Override
	public List<RunLimitTable> getList() {
		return _list;
	}
	
	@Override
	public int mapRow(ResultSet rs, int rowNum) throws SQLException {
		RunLimitTable t = new RunLimitTable();
		
		t.setItmIdx(rs.getInt(1));
		t.setItemName(rs.getString(2));
		
		_list.add(t);
		
		return 1;
	}
	
}
