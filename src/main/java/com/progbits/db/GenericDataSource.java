package com.progbits.db;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;

public class GenericDataSource implements DataSource {
	private org.slf4j.Logger log = LoggerFactory
			.getLogger(GenericDataSource.class);

	private String serverUrl;
	private String userName;
	private String password;
	private String driver;

	private boolean driverLoaded = false;

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return null;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return 0;
	}

	@Override
	public java.util.logging.Logger getParentLogger()
			throws SQLFeatureNotSupportedException {
		return null;
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws SQLException {
		
	}

	@Override
	public void setLoginTimeout(int arg0) throws SQLException {
		
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		if (!driverLoaded) {
			try {
				Class.forName(driver);

				driverLoaded = true;
			} catch (Exception ex) {
				log.error("getConnection Load Driver", ex);

				return null;
			}
		}

		return DriverManager.getConnection(serverUrl, userName, password);
	}

	@Override
	public Connection getConnection(String lclUser, String lclPass)
			throws SQLException {
		if (!driverLoaded) {
			try {
				Class.forName(driver);

				driverLoaded = true;
			} catch (Exception ex) {
				log.error("getConnection Load Driver", ex);

				return null;
			}
		}

		return DriverManager.getConnection(serverUrl, lclUser, lclPass);
	}

}
