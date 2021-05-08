package com.progbits.db;

import java.sql.SQLException;
import java.sql.Connection;

/**
 * Used in try with resources to set Auto Commit state, and reset after done
 */
public class AutoSetAutoCommit implements AutoCloseable {

    private final Connection conn;
    private final boolean originalAutoCommit;

    public AutoSetAutoCommit(Connection conn, boolean autoCommit) throws SQLException {
        this.conn = conn;
        originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(autoCommit);
    }

    @Override
    public void close() throws SQLException {
        conn.setAutoCommit(originalAutoCommit);
    }

}