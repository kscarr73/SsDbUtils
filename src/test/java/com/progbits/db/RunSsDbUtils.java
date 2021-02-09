/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.progbits.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.testng.annotations.Test;

/**
 *
 * @author KCarr
 */
public class RunSsDbUtils {

	public Connection getConnection() {
		String url = "jdbc:jtds:sqlserver://localhost/MAS90MNGTRPTS";
		String userName = "sa";
		String password = "GG07171997";

		Connection conn = null;

		try {
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			
			conn = DriverManager.getConnection(url, userName, password);

			Integer iCnt = 0;
			try {
				iCnt = SsDbUtils.queryForInt(conn, "SELECT COUNT(*) FROM testLimitTable", new Object[]{});
			} catch (Exception ex) {
			}

			if (iCnt == 0) {
				setupLimitTable(conn);
			}

			return conn;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}

	}

	private void setupLimitTable(Connection conn) {
		String strSQL = "CREATE TABLE testLimitTable ( "
				+ "itmIdx int,"
				+ "entryName VARCHAR(25)"
				+ ")";

		try {
			SsDbUtils.update(conn, strSQL, new Object[]{});

			strSQL = "INSERT INTO testLimitTable(itmIdx,entryName) "
					+ "VALUES (?,?)";
			for (int x = 1; x <= 100; x++) {
				SsDbUtils.update(conn, strSQL, new Object[] { x, "Test" + x });
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Test
	public void testLimit() throws Exception {
		Connection conn = null;
		RunLimitTableMapper map = new RunLimitTableMapper();
		
		try {
			conn = getConnection();
			
			SsDbUtils.queryLimit(conn, map.getSql(), new Object[] {}, map, 0, 25);
			
			List<RunLimitTable> recs = map.getList();
			
			assert recs != null;
			
			assert recs.size() == 25 : "Total Found: " + recs.size();
			
			assert recs.get(0).getItmIdx() == 1 : "First Record Found: " + recs.get(0).getItmIdx();
			
			assert recs.get(24).getItmIdx() == 25 : "Last Record Found: " + recs.get(24).getItmIdx();
			
			RunLimitTableMapper map2 = new RunLimitTableMapper();
			
			SsDbUtils.queryLimit(conn, map.getSql(), new Object[] {}, map2, 75, 25);
			
			recs = map2.getList();
			
			assert recs != null;
			
			assert recs.size() == 25 : "Total Found: " + recs.size();
			
			assert recs.get(0).getItmIdx() == 76 : "First Record Found: " + recs.get(0).getItmIdx();
			
			assert recs.get(24).getItmIdx() == 100 : "Last Record Found: " + recs.get(24).getItmIdx();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			SsDbUtils.closeConnection(conn);
		}
	}
}
