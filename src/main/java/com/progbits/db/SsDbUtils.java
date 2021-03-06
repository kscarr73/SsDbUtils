package com.progbits.db;

import java.math.BigDecimal;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } else {
            ps = conn.prepareStatement(sql);
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
        Tuple<PreparedStatement, ResultSet> ret = new Tuple<PreparedStatement, ResultSet>();

        String dbType = conn.getMetaData().getDatabaseProductName();

        boolean bForceStart = dbType.equals("Microsoft SQL Server");

        String strSQL = "";

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
                return new Long(-1);
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
        List<String> fieldNames = new ArrayList<String>();

        int iCnt = rs.getMetaData().getColumnCount();

        for (int x = 1; x <= iCnt; x++) {
            fieldNames.add(rs.getMetaData().getColumnName(x));
        }

        return fieldNames;
    }
}
