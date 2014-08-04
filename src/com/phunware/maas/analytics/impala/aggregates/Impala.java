package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

public class Impala {

	private final static Map<String,String> queryMap = new HashMap<String,String>();
	
	public static Connection getConnection(final Configuration config) throws ClassNotFoundException, SQLException {
		Class.forName(config.get("hiveDriver"));
		String connectString = "jdbc:hive2://"+config.get("hiveHostName")+":"+config.get("impalaPort")+"/";
		if ("YES".equals(config.get("impalaKerberos"))) {
			connectString = connectString + config.get("impalaDatabaseName")+";principal="+config.get("impalaPrincipal");
		}
		else {
			connectString = connectString + ";auth=noSasl";
		}
		if ("YES".equals(config.get("verbose"))) {
			System.out.println("JDBC connect string is: " + connectString); 
		}
		if ("YES".equals(config.get("hiveKerberos"))) {
			return DriverManager.getConnection(connectString);
		} else {
			return DriverManager.getConnection(connectString); //, "maasdev", "");
		}
	}
	
	public static void doStatement(final Connection connection, final String sql, final boolean printSql) throws SQLException {
		final Statement stmt = connection.createStatement();
		//System.out.println(sql);
		final String executableSQL = getCleanStatement(connection, sql);
		try {
			if (printSql) {
				System.out.println(executableSQL);
			}
			stmt.execute(executableSQL);
		} finally {
			stmt.close();
		}
	}
	
	private static String getCleanStatement(final Connection connection, final String statement) throws SQLException {
		String executableStatement = statement;
		final Map<String,String> varMap = new HashMap<String,String>();
        final Pattern p = Pattern.compile("(\\$[^;]+;)");
        final Matcher m = p.matcher(statement);
        int offset = 0;
        while (m.find(offset)) {
          	varMap.put(m.group(0), "");
          	offset = m.end(0);
        }
   		for (final String key : varMap.keySet()) {
			final String sql = key.replace("$",  "").replace(";", "");
			final String value = getFirstValue(connection, sql); 
			executableStatement = executableStatement.replace(key, "'" + value + "'");
    	}
        return executableStatement;
	}
	
	private static String getFirstValue(final Connection connection, final String sql) throws SQLException {
		if (queryMap.containsKey(sql)) {
			return queryMap.get(sql);
		}
		
		final Statement stmt = connection.createStatement();
   		try {
			final ResultSet results = stmt.executeQuery(sql);
   			if (isMyResultSetEmpty(results)) {
   				queryMap.put(sql,"");
   				return "";
   			}
   			results.next();
   			final String value = results.getString(1);
   			queryMap.put(sql, value);
   			return value;
   		} finally {
			stmt.close();
		}
	}
	
	private static boolean isMyResultSetEmpty(final ResultSet rs) throws SQLException {
	    return !rs.isBeforeFirst() && rs.getRow() == 0;
	}
	
	public static void verifyTableExists(final Connection connection, final String database, final String table) throws SQLException {
		try {
			doStatement(connection, "invalidate metadata " + database + "." + table, false);
			doStatement(connection, "refresh " + database + "." + table, false);
		} catch (SQLException e) {
			throw new SQLException ("Impala table " + database + "." + table + " does not exist.");
		}
		if (!findTable(connection, database, table)) {
			throw new SQLException ("Impala table " + database + "." + table + " does not exist.");
		}
	}
	
	private static boolean findTable(final Connection connection, final String database, final String table) throws SQLException {
		final Statement stmt = connection.createStatement();
		try {
			final ResultSet res = stmt.executeQuery("show tables in " + database + " like '" + table + "'");
			try {
				while (res.next()) {
					final String tableName = res.getString(1);
					if (tableName.equals(table)) {
						return true;
					}
				}
			}
			finally {
				res.close();
			}
		} finally {
			stmt.close();
		}
		return false;
	}
	
	public static void dropTable(final Connection connection, final String tableName, final boolean verbose) throws SQLException {
		final String sql = "DROP TABLE IF EXISTS " + tableName;
		doStatement(connection, sql, verbose);
	}
	
	public static void createTable(final Connection connection, final String tableName, final String tableDef, final boolean verbose) throws SQLException {
		final String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " " + tableDef;
		doStatement(connection, sql, verbose);
	}
	
	public static void updateTable(final Connection connection, 
			final boolean verbose, final boolean setup, final boolean rebuild,
			final String tableName, final String tableDef, final String tableUpdate) throws SQLException {
		if (rebuild) {
			dropTable(connection, tableName, verbose);
		}
		if (setup || rebuild) {
			createTable(connection, tableName, tableDef, verbose);
		} else {
			doStatement(connection, "refresh " + tableName, verbose);
		}
		doStatement(connection, tableUpdate, verbose);
	}
	
}