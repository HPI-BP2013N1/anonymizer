package de.hpi.bp2013n1.anonymizer.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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


public class StandardSqlHelper extends SQLHelper {

	@Override
	public String truncateTable(String qualifiedTableName) {
		return String.format("TRUNCATE TABLE %s", qualifiedTableName);
	}

	@Override
	public boolean supportsDisableAllForeignKeys() {
		return false;
	}

	@Override
	public String disableAllForeignKeys() {
		throw new UnsupportedOperationException(
				"Disabling constraints is not standardized in SQL");
	}

	@Override
	public String enableAllForeignKeys() {
		throw new UnsupportedOperationException(
				"Disabling constraints is not standardized in SQL");
	}

	@Override
	public String disableForeignKey(String qualifiedTableName, String constraintName) {
		throw new UnsupportedOperationException(
				"Disabling constraints is not standardized in SQL");
	}

	@Override
	public String enableForeignKey(String qualifiedTableName, String constraintName) {
		throw new UnsupportedOperationException(
				"Disabling constraints is not standardized in SQL");
	}

	@Override
	public Object takeConstant(Connection connection, String expression) throws SQLException {
		try (PreparedStatement selectStatement = connection.prepareStatement(
				"SELECT " + expression);
				ResultSet resultSet = selectStatement.executeQuery()) {
			resultSet.next();
			return resultSet.getObject(1);
		}
	}

}
