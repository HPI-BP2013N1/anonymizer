package de.hpi.bp2013n1.anonymizer.util;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI-BP2013N1
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.sql.Connection;
import java.sql.SQLException;

public abstract class SQLHelper {
	public static SQLHelper getHelperFor(Connection connection)
			throws SQLException {
		String jdbcURL = connection.getMetaData().getURL();
		if (jdbcURL.startsWith("jdbc:db2:")) {
			return DB2_IMPLEMENTATION;
		} else if (jdbcURL.startsWith("jdbc:h2:")) {
			return H2_IMPLEMENTATION;
		} else {
			return DEFAULT_IMPLEMENTATION;
		}
	}
	
	protected static SQLHelper DB2_IMPLEMENTATION = new DB2SqlHelper();
	protected static SQLHelper H2_IMPLEMENTATION = new H2SqlHelper();
	protected static SQLHelper DEFAULT_IMPLEMENTATION = new StandardSqlHelper();
	
	public abstract String truncateTable(String qualifiedTableName);
	
	public static String truncateTable(Connection connection, 
			String qualifiedTableName) throws SQLException {
		return getHelperFor(connection).truncateTable(qualifiedTableName);
	}

	public abstract boolean supportsDisableAllForeignKeys();

	public static boolean supportsDisableAllForeignKeys(Connection connection) 
			throws SQLException {
		return getHelperFor(connection).supportsDisableAllForeignKeys();
	}
	
	public abstract String disableAllForeignKeys();

	public static String disableAllForeignKeys(Connection connection)
			throws SQLException {
		return getHelperFor(connection).disableAllForeignKeys();
	}

	public abstract String enableAllForeignKeys();

	public static String enableAllForeignKeys(Connection connection) 
			throws SQLException {
		return getHelperFor(connection).enableAllForeignKeys();
	}

	public abstract String disableForeignKey(String qualifiedTableName,
			String constraintName);

	public static String disableForeignKey(Connection connection,
			String qualifiedTableName, String constraintName)
			throws SQLException {
		return getHelperFor(connection).disableForeignKey(qualifiedTableName,
				constraintName);
	}

	public abstract String enableForeignKey(String qualifiedTableName,
			String constraintName);

	public static String enableForeignKey(Connection connection,
			String qualifiedTableName, String constraintName)
			throws SQLException {
		return getHelperFor(connection).enableForeignKey(qualifiedTableName,
				constraintName);
	}
	
	public abstract Object takeConstant(Connection connection, String expression) throws SQLException;
	
	public static Object selectConstant(Connection connection,
			String expression) throws SQLException {
		return getHelperFor(connection).takeConstant(connection, expression);
	}
	
	public abstract void createSchema(Connection connection, String schema) 
			throws SQLException;

	public static String qualifiedTableName(String schema, String table) {
		return schema + "." + table;
	}

	public static void createSchema(String schema, Connection connection) 
			throws SQLException {
		getHelperFor(connection).createSchema(connection, schema);
	}
	
}
