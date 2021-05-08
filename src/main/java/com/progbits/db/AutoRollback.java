package com.progbits.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Used in try with Resources to Auto Rollback if commit is not called
 * 
 */
public class AutoRollback implements AutoCloseable {

    private Connection conn;
    private boolean committed;

    public AutoRollback(Connection conn) throws SQLException {
        this.conn = conn;        
    }

    public void commit() throws SQLException {
        conn.commit();
        committed = true;
    }

    @Override
    public void close() throws SQLException {
        if(!committed) {
            conn.rollback();
        }
    }

}
