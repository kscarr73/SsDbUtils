package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scarr
 */
public class SsDbUtils {

	private static final Logger log = LoggerFactory.getLogger(SsDbUtils.class);

	public static PreparedStatement returnStatement(Connection conn, String sql) throws Exception {
		PreparedStatement ps = null;

		if (log.isTraceEnabled()) {
			log.trace("Requested SQL: {}", sql);
		}

		String dbType = getDbType(conn);

		if (dbType.equals("Microsoft SQL Server")) {
			ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} else {
			ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}

		if ("PostgreSQL".equals(dbType)) {
			ps.setFetchSize(50);
		}

		return ps;
	}

	public static PreparedStatement returnStatement(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = returnStatement(conn, sql);

		populateParameters(ps, args);

		return ps;
	}

	public static void populateParameters(PreparedStatement ps, Object[] args) throws Exception {
		for (int x = 0; x < args.length; x++) {
			Object o = args[x];

			if (o instanceof OffsetDateTime) {
				OffsetDateTime dt = (OffsetDateTime) o;

				Timestamp ts = new Timestamp(dt.toInstant().toEpochMilli());

				ps.setObject(x + 1, ts);
			} else {
				ps.setObject(x + 1, o);
			}

			if (log.isTraceEnabled()) {
				log.trace("Argument: {}", o);
			}
		}
	}

	public static Tuple<PreparedStatement, ResultSet> returnResultset(Connection conn, String sql, Object[] args) throws Exception {
		Tuple<PreparedStatement, ResultSet> ret = new Tuple<>();

		ret.setFirst(returnStatement(conn, sql, args));
		ret.setSecond(ret.getFirst().executeQuery());

		return ret;
	}

	public static Tuple<PreparedStatement, ResultSet> returnResultset(
			Connection conn, String sql, Object[] args, int iStart, int iCount)
			throws Exception {
		Tuple<PreparedStatement, ResultSet> ret = new Tuple<>();

		String dbType = conn.getMetaData().getDatabaseProductName();

		boolean bForceStart = dbType.equals("Microsoft SQL Server");

		String strSQL;

		if (iCount > 0) {
			if (bForceStart) {
				strSQL = addDbLimit(dbType, sql, iStart + iCount, iCount);
			} else {
				strSQL = addDbLimit(dbType, sql, iStart, iCount);
			}
		} else {
			strSQL = sql;
		}

		ret.setFirst(returnStatement(conn, strSQL, args));

		ret.setSecond(ret.getFirst().executeQuery());

		if (bForceStart && iStart > 0) {

			if (log.isTraceEnabled()) {
				log.trace("Moving to {} row", iStart);
			}

			int iTestCnt = 0;
			while (ret.getSecond().next()) {
				if (iTestCnt == iStart) {
					break;
				}
				iTestCnt++;
			}
		}

		return ret;
	}

	/**
	 * Returns the First Column of the First Row of the results from a SQL
	 * Statement
	 *
	 * Can be used to return lookups fast.
	 *
	 * @param conn Connection to use for the request
	 * @param sql SQL to run
	 * @param args Arguments to pass into the request
	 *
	 * @return the integer that was found
	 */
	public static Integer queryForInt(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;

		try {
			ps = returnStatement(conn, sql);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			return queryForInt(ps, args);
		} finally {
			closePreparedStatement(ps);
		}
	}

	/**
	 * Returns the First Column of the First Row of the results from a SQL
	 * Statement
	 *
	 * Can be used to return lookups fast.
	 *
	 * @param ps PreparedStatement to use for this query
	 * @param args Arguments to pass into the request
	 *
	 * @return the integer that was found
	 */
	public static Integer queryForInt(PreparedStatement ps, Object[] args) throws Exception {
		ResultSet rs = null;

		try {
			populateParameters(ps, args);

			rs = ps.executeQuery();

			if (!rs.isClosed()) {
				if (rs.next()) {
					return rs.getInt(1);
				} else {
					return null;
				}
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
		}
	}

	public static Long queryForLong(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getLong(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	public static BigDecimal queryForDecimal(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getBigDecimal(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	/**
	 * Returns the First Column of the First Row of the results from a SQL
	 * Statement
	 *
	 * Can be used to return lookups fast.
	 *
	 * @param conn Connection to use for the request
	 * @param sql SQL to run
	 * @param args Arguments to pass into the request
	 *
	 * @return the Timestamp that was found
	 */
	public static Timestamp queryForTimestamp(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getTimestamp(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	/**
	 * Returns the First Column of the First Row of the results from a SQL
	 * Statement
	 *
	 * Can be used to return lookups fast.
	 *
	 * @param conn Connection to use for the request
	 * @param sql SQL to run
	 * @param args Arguments to pass into the request
	 *
	 * @return the Date that was found
	 */
	public static Date queryForDate(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getDate(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	/**
	 * Returns the First Column of the First Row of the results from a SQL
	 * Statement
	 *
	 * Can be used to return lookups fast.
	 *
	 * @param conn Connection to use for the request
	 * @param sql SQL to run
	 * @param args Arguments to pass into the request
	 *
	 * @return the String that was found
	 */
	public static String queryForString(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getString(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	/**
	 * Returns the First Column of the First Row of the results from a Prepared
	 * Statement
	 *
	 * <p>
	 * Can be used to return lookups fast.</p>
	 *
	 * @param ps Prepared Statement that has SQL already assigned
	 * @param args Arguments to pass into the request
	 *
	 * @return the String that was found
	 */
	public static String queryForString(PreparedStatement ps, Object[] args) throws Exception {
		ResultSet rs = null;

		try {
			populateParameters(ps, args);

			rs = ps.executeQuery();

			if (rs.next()) {
				return rs.getString(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
		}
	}

	public static List<Object> queryForRow(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = returnStatement(conn, sql, args);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = ps.executeQuery();

			if (rs.next()) {
				List<Object> obj = new ArrayList<Object>();

				for (int xrow = 1; xrow <= rs.getMetaData().getColumnCount(); xrow++) {
					obj.add(rs.getObject(xrow));
				}

				return obj;
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(ps);
		}
	}

	public static List<Object> queryForRow(PreparedStatement ps, Object[] args) throws Exception {
		ResultSet rs = null;

		try {
			populateParameters(ps, args);

			rs = ps.executeQuery();

			if (rs.next()) {
				List<Object> obj = new ArrayList<Object>();

				for (int xrow = 1; xrow <= rs.getMetaData().getColumnCount(); xrow++) {
					obj.add(rs.getObject(xrow));
				}

				return obj;
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
		}
	}

	public static List<Map<String, Object>> queryForAllRows(Connection conn, String sql, Object[] args) throws Exception {
		Tuple<PreparedStatement, ResultSet> rs = null;

		try {
			rs = returnResultset(conn, sql, args);
			List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

			while (rs.getSecond().next()) {
				Map<String, Object> row = new HashMap<String, Object>();

				for (int xrow = 1; xrow <= rs.getSecond().getMetaData().getColumnCount(); xrow++) {
					String colName = rs.getSecond().getMetaData().getColumnName(xrow);

					row.put(colName, rs.getSecond().getObject(xrow));
				}

				rows.add(row);
			}

			return rows;
		} finally {
			closeTuple(rs);
		}
	}

	/**
	 * Run an Insert Statement and return the Key created
	 *
	 * @param conn Connection to use to run the statement
	 * @param sql SQL Statement to run
	 * @param args Arguments to replace, must match the same number of ? in SQL
	 * Statement
	 * @param keys Keys expected to be generated
	 *
	 * @return Returns the Key created with the insert
	 */
	public static Integer insertWithKey(Connection conn, String sql, Object[] args, String[] keys) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			keys = getGeneratedKeys(conn, keys);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			stmt = conn.prepareStatement(sql, keys);

			populateParameters(stmt, args);

			stmt.execute();

			rs = stmt.getGeneratedKeys();

			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return -1;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(stmt);
		}
	}

	public static Long insertWithLongKey(Connection conn, String sql, Object[] args, String[] keys) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			keys = getGeneratedKeys(conn, keys);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			stmt = conn.prepareStatement(sql, keys);

			populateParameters(stmt, args);

			stmt.execute();

			rs = stmt.getGeneratedKeys();

			if (rs.next()) {
				return rs.getLong(1);
			} else {
				return -1L;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(stmt);
		}
	}

	public static String insertWithStringKey(Connection conn, String sql, Object[] args, String[] keys) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			keys = getGeneratedKeys(conn, keys);

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			stmt = conn.prepareStatement(sql, keys);

			populateParameters(stmt, args);

			stmt.execute();

			rs = stmt.getGeneratedKeys();

			if (rs.next()) {
				return rs.getString(1);
			} else {
				return null;
			}
		} finally {
			closeResultSet(rs);
			closePreparedStatement(stmt);
		}
	}

	/**
	 * Run an Insert Statement and return the Key created
	 *
	 * @param conn Connection to use to run the statement
	 * @param sql SQL Statement to run
	 * @param args Arguments to replace, must match the same number of ? in SQL
	 * Statement
	 *
	 * @return Returns the Key created with the insert
	 */
	public static boolean update(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement stmt = null;

		try {
			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			stmt = returnStatement(conn, sql, args);

			stmt.execute();

			return true;
		} finally {
			closePreparedStatement(stmt);
		}
	}

	/**
	 * Run an Insert Statement and return the number of entries modified
	 *
	 * @param conn Connection to use to run the statement
	 * @param sql SQL Statement to run
	 * @param args Arguments to replace, must match the same number of ? in SQL
	 * Statement
	 *
	 * @return Returns the number of entries modified by the statement
	 *
	 * @throws Exception if there is any exception during the statement
	 */
	public static int updateWithCount(Connection conn, String sql, Object[] args) throws Exception {
		PreparedStatement stmt = null;

		try {
			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			stmt = returnStatement(conn, sql, args);

			return stmt.executeUpdate();
		} finally {
			closePreparedStatement(stmt);
		}
	}

	/**
	 * Run an Insert Statement and return the Key created
	 *
	 * @param ps Prepared Statement to run against
	 * @param args Arguments to replace, must match the same number of ? in SQL
	 * Statement
	 *
	 * @return Returns the Key created with the insert
	 *
	 * @throws Exception
	 */
	public static boolean update(PreparedStatement ps, Object[] args) throws Exception {
		populateParameters(ps, args);

		ps.execute();

		return true;
	}

	/**
	 * Runs a Query against a Connection with specific arguments, and maps the
	 * results to a List using a RowMapper
	 *
	 * @param conn Connection to use with the query. NOTE: Does not close
	 * connection
	 * @param sql SQL to run
	 * @param args Arguments to run with sql. Arguments must match types for ?
	 * replacement.
	 * @param map RowMapper to map the results and return a List
	 *
	 * @return Number of rows returned
	 *
	 * @throws Exception
	 */
	public static int query(Connection conn, String sql, Object[] args, RowMapper<?> map) throws Exception {
		Tuple<PreparedStatement, ResultSet> rs = null;

		try {
			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", sql);
			}

			rs = returnResultset(conn, sql, args);

			int rowCnt = 0;

			while (rs.getSecond().next()) {
				map.mapRow(rs.getSecond(), rowCnt);

				rowCnt++;
			}

			return rowCnt;
		} finally {
			closeTuple(rs);
		}
	}

	/**
	 * Runs a Query against a Connection with specific arguments, and maps the
	 * results to a List using a RowMapper, with using a Limit clause
	 *
	 * @param conn Connection to use with the query. NOTE: Does not close
	 * connection
	 * @param sql SQL to run
	 * @param args Arguments to run with sql. Arguments must match types for ?
	 * replacement.
	 * @param map RowMapper to map the results and return a List
	 * @param iStart Offset to start pulling mapping records
	 * @param iCount Count of Records to return
	 *
	 * @return List of the Type of the RowMapper passed in
	 *
	 * @throws Exception
	 */
	public static int queryLimit(Connection conn, String sql, Object[] args, RowMapper<?> map, int iStart, int iCount) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			String dbType = conn.getMetaData().getDatabaseProductName();

			boolean bForceStart = dbType.equals("Microsoft SQL Server");

			if (log.isTraceEnabled()) {
				log.trace("Db Type: {} Force Start: {}", dbType, bForceStart);
			}
			String strSQL = "";

			if (bForceStart) {
				strSQL = addDbLimit(dbType, sql, iStart + iCount, iCount);
			} else {
				strSQL = addDbLimit(dbType, sql, iStart, iCount);
			}

			if (log.isTraceEnabled()) {
				log.trace("Requested SQL: {}", strSQL);
			}

			ps = conn.prepareStatement(strSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			populateParameters(ps, args);

			rs = ps.executeQuery();

			if (bForceStart && iStart > 0) {
				if (log.isTraceEnabled()) {
					log.trace("Moving to {} row", iStart);
				}

				rs.absolute(iStart);
			}

			int rowCnt = 1;

			while (rs.next()) {
				map.mapRow(rs, rowCnt);

				rowCnt++;
			}

			return rowCnt;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}

				if (ps != null) {
					ps.close();
				}
			} catch (Exception ex) {
				log.error("query: Close Exception", ex);
			}
		}
	}

	/**
	 * Update a SQL statement with the Limit Clause for various databases
	 *
	 * @param dbType Type of Database retrieved with:
	 * conn.getMetaData().getDatabaseProductName()
	 * @param sql SQL Statement to update
	 * @param iStart Offset to start pulling records
	 * @param iCount Records to return
	 *
	 * @return Updated SQL Statement with limits
	 */
	public static String addDbLimit(String dbType, String sql, int iStart, int iCount) {
		try {
			if ("Microsoft SQL Server".equals(dbType)) {
				return sql.replace("SELECT ", "SELECT TOP " + iStart + " ");
			} else if (dbType.equals("MySQL") || dbType.equals("PostgreSQL")) {
				return sql + " LIMIT " + iCount + " OFFSET " + iStart;
			} else {
				log.error("Db Type: {} not found", dbType);

				return sql;
			}
		} catch (Exception ex) {
			log.error("addDbLimit", ex);

			return sql;
		}
	}

	public static String getDbType(Connection conn) {
		try {
			return conn.getMetaData().getDatabaseProductName();
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * Return Generated Keys object based on DB Type
	 *
	 * @param conn Connection that should be checked for DB Type
	 * @param keys List of strings to return
	 *
	 * @return Updated Keys for the proper database
	 */
	public static String[] getGeneratedKeys(Connection conn, String[] keys) {
		String[] keyRet = new String[keys.length];

		try {
			String dbType = getDbType(conn);

			if (dbType != null && dbType.equals("PostgreSQL")) {
				for (int x = 0; x < keys.length; x++) {
					keyRet[x] = keys[x].toLowerCase();
				}
			} else {
				keyRet = keys.clone();
			}
		} catch (Exception ex) {
			log.error("getGeneratedKeys", ex);
		}

		return keyRet;
	}

	public static void closePreparedStatement(PreparedStatement ps) {
		try {
			if (ps != null) {
				ps.close();
			}
		} catch (Exception ex) {
			log.error("closePreparedStatement", ex);
		}
	}

	public static void closeTuple(Tuple<PreparedStatement, ResultSet> tpl) {
		if (tpl != null) {
			closeResultSet(tpl.getSecond());
			closePreparedStatement(tpl.getFirst());
		}
	}

	public static void closeResultSet(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (Exception ex) {
			log.error("closeResultSet", ex);
		}
	}

	public static void closeConnection(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception ex) {
			log.error("closeConnection", ex);
		}
	}

	public static void rollbackConnection(Connection conn) {
		try {
			if (conn != null) {
				conn.rollback();
			}
		} catch (Exception ex) {
			log.error("rollbackConnection", ex);
		}
	}

	public static List<String> getFieldNames(ResultSet rs) throws Exception {
		List<String> fieldNames = new ArrayList<>();

		int iCnt = rs.getMetaData().getColumnCount();

		for (int x = 1; x <= iCnt; x++) {
			fieldNames.add(rs.getMetaData().getColumnLabel(x));
		}

		return fieldNames;
	}

	private static void queryForAllRowsIntoApiObject(Connection conn, String sql, Object[] args, ApiObject subject) throws Exception {
		Tuple<PreparedStatement, ResultSet> rs = null;

		try {
			long lStartTime = System.currentTimeMillis();
			rs = returnResultset(conn, sql, args);
			log.debug("SQL Query: {}", System.currentTimeMillis() - lStartTime);

			lStartTime = System.currentTimeMillis();
			List<ApiObject> root;

			if (!subject.isSet("root")) {
				subject.createList("root");
			}

			root = subject.getList("root");

			int iColumnCount = rs.getSecond().getMetaData().getColumnCount();

			while (rs.getSecond().next()) {
				ApiObject row = new ApiObject();

				for (int xrow = 1; xrow <= iColumnCount; xrow++) {
					String colName = rs.getSecond().getMetaData().getColumnLabel(xrow);

                                        Object objValue = rs.getSecond().getObject(xrow);
                                        
					row.put(colName, convertObject(objValue));
				}

				root.add(row);
			}

			log.debug("SQL Pull Query: {}", System.currentTimeMillis() - lStartTime);
		} finally {
			closeTuple(rs);
		}
	}

	/**
	 * Executes a SQL Statement and Returns a Valid ApiObject
	 *
	 * This is used
	 *
	 * @param conn Connection to run the SQL Against
	 * @param sql SQL Statement to run
	 * @param args Arguments to use in the SQL Statement
	 * @return ApiObject with the column names as field Names in a list named
	 * "root"
	 *
	 * @throws ApiException
	 */
	public static ApiObject querySqlAsApiObject(Connection conn, String sql, Object[] args) throws ApiException {
		ApiObject retObj = new ApiObject();

		try {
			retObj.createList("root");

			SsDbUtils.queryForAllRowsIntoApiObject(conn, sql, args, retObj);
		} catch (Exception ex) {
			processException(ex);
		}

		return retObj;
	}

	protected static String gatherParamsForSql(String sql, List<Object> params, ApiObject args) throws ApiException {
		Matcher matcher = sqlPattern.matcher(sql);

		//List<String> foundItems = new ArrayList<>();
		String lclSql = sql;

		while (matcher.find()) {
			String found = matcher.group(2);

			if (args.containsKey(found)) {
				switch (args.getType(found)) {
					case ApiObject.TYPE_STRINGARRAY:
						StringBuilder replaceVal = new StringBuilder();

						AtomicBoolean blnFirst = new AtomicBoolean(true);

						args.getStringArray(found).forEach((val) -> {
							params.add(val);
							if (!blnFirst.get()) {
								replaceVal.append(",");
							} else {
								blnFirst.set(false);
							}

							replaceVal.append("?");
						});

						lclSql = lclSql.replaceFirst(":" + found, replaceVal.toString());

						break;

					case ApiObject.TYPE_INTEGERARRAY:
						StringBuilder replaceVal2 = new StringBuilder();

						AtomicBoolean blnFirst2 = new AtomicBoolean(true);

						args.getIntegerArray(found).forEach((val) -> {
							params.add(val);
							if (!blnFirst2.get()) {
								replaceVal2.append(",");
							} else {
								blnFirst2.set(false);
							}

							replaceVal2.append("?");
						});

						lclSql = lclSql.replaceFirst(":" + found, replaceVal2.toString());
						break;

					default:
						params.add(args.get(found));
						lclSql = lclSql.replaceFirst(":" + found, "?");
				}
			} else {
				throw new ApiException(400, "Argument: " + found + " Doesn't Exist");
			}
		}

		return lclSql;
	}

	/**
	 * Executes a SQL Statement and Returns a Valid ApiObject
	 *
	 * <p>
	 * <b>Example:</b> SELECT * FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn Connection to run the SQL Against
	 * @param sql The parameterized SQL
	 * @param args Arguments to use in the SQL Statement
	 * @return ApiObject with the column names as field Names in a list named
	 * "root"
	 *
	 * @throws ApiException
	 */
	public static ApiObject querySqlAsApiObject(Connection conn, String sql, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		return querySqlAsApiObject(conn, lclSql, params.toArray());
	}

	protected static Object convertObject(Object v) {
		if (v instanceof Timestamp) {
			Instant instant = Instant.ofEpochMilli(((Timestamp) v).getTime());
			return OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
		} else if (v instanceof Short) {
			return ((Short) v).intValue();
                } else if (v instanceof BigDecimal) {
                    BigDecimal b = (BigDecimal) v;
                    
                    if (b.scale() == 0) {
                        return b.longValue();
                    } else {
                        return b.doubleValue();
                    }
		} else {
			return v;
		}
	}

	protected static Pattern sqlPattern = Pattern.compile("(:(.[^\\s,);]*))");

	/**
	 * Uses an ApiObject to pass the parameter names to the SQL statement.
	 *
	 * Use :name to set the arguments that should be replaced.
	 *
	 * <p>
	 * <b>Example:</b> SELECT * FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn The connection to use to make the call to SQL
	 * @param sql The parameterized SQL
	 * @param args ApiObject with the fields to send to the SQL.
	 * @return true/false if the call worked
	 * @throws ApiException Argument exception if the argument doesn't exist in
	 * args, SQL exception
	 */
	public static Boolean updateObject(Connection conn, String sql, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		try {
			return update(conn, lclSql, params.toArray());
		} catch (Exception ex) {
			processException(ex);
		}
		
		throw null;
	}

	/**
	 * Uses an ApiObject to pass the parameter names to the SQL statement.
	 *
	 * Use :name to set the arguments that should be replaced.
	 *
	 * <p>
	 * <b>Example:</b> SELECT * FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn The connection to use to make the call to SQL
	 * @param sql The parameterized SQL
	 * @param args ApiObject with the fields to send to the SQL.
	 * @return Integer with the number of records that were modified
	 * @throws ApiException Argument exception if the argument doesn't exist in
	 * args, SQL exception
	 */
	public static Integer updateObjectWithCount(Connection conn, String sql, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		try {
			return updateWithCount(conn, lclSql, params.toArray());
		} catch (Exception ex) {
			processException(ex);
		}
		
		throw null;
	}

	/**
	 * Uses an ApiObject to pass the parameter names to the SQL statement.
	 *
	 * Use :name to set the arguments that should be replaced.
	 *
	 * <p>
	 * <b>Example:</b> SELECT * FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn The connection to use to make the call to SQL
	 * @param sql The parameterized SQL
	 * @param keys The names of the keys to capture the new ID from
	 * @param args ApiObject with the fields to send to the SQL.
	 * @return Integer value of the inserted Key field
	 * @throws ApiException Argument exception if the argument doesn't exist in
	 * args, SQL exception
	 */
	public static Integer insertObjectWithKey(Connection conn, String sql, String[] keys, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		try {
			return insertWithKey(conn, lclSql, params.toArray(), keys);
		} catch (Exception ex) {
			processException(ex);
		}
		
		return null;
	}

	/**
	 * Uses an ApiObject to pass the parameter names to the SQL statement.
	 *
	 * Use :name to set the arguments that should be replaced.
	 *
	 * <p>
	 * <b>Example:</b> SELECT * FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn The connection to use to make the call to SQL
	 * @param sql The parameterized SQL
	 * @param keys The names of the keys to capture the new ID from
	 * @param args ApiObject with the fields to send to the SQL.
	 * @return Integer value of the inserted Key field
	 * @throws ApiException Argument exception if the argument doesn't exist in
	 * args, SQL exception
	 */
	public static String insertObjectWithStringKey(Connection conn, String sql, String[] keys, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		try {
			return insertWithStringKey(conn, lclSql, params.toArray(), keys);
		} catch (Exception ex) {
			processException(ex);
		}
		
		throw null;
	}

	/**
	 * Uses an ApiObject to pass the parameter names to the SQL statement.
	 *
	 * Use :name to set the arguments that should be replaced.
	 *
	 * <p>
	 * <b>Example:</b> SELECT id FROM tableName WHERE firstName=:firstName</p>
	 *
	 * @param conn The connection to use to make the call to SQL
	 * @param sql The parameterized SQL
	 * @param args ApiObject with the fields to send to the SQL.
	 * @return The first row, the first column as an Integer
	 * @throws ApiException Argument exception if the argument doesn't exist in
	 * args, SQL exception
	 */
	public static Integer queryObjectForInt(Connection conn, String sql, ApiObject args) throws ApiException {
		List<Object> params = new ArrayList<>();

		String lclSql = gatherParamsForSql(sql, params, args);

		try {
			return queryForInt(conn, lclSql, params.toArray());
		} catch (Exception ex) {
			processException(ex);
		}
		
		throw null;
	}

	public static void processException(Exception ex) throws ApiException {
		if (ex instanceof SQLException) {
			SQLException sqlEx = (SQLException) ex;

			if (sqlEx.getMessage() != null) {
				if (sqlEx.getMessage().contains("Duplicate")) {
					throw new ApiException(409, "Duplicate Record");
				} else {
					throw new ApiException(400, "SQL: " + ex.getMessage());
				}
			} else {
				throw new ApiException(500, "SQL Exception");
			}
		} else if (ex != null && ex.getMessage() != null) {
			throw new ApiException(400, "SQL: " + ex.getMessage());
		} else {
			throw new ApiException(400, "No Information Error", ex);
		}
	}
}
