package de.hpi.bp2013n1.anonymizer.tools;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI Bachelor's Project N1 2013
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.Constraint;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

public class TableTruncater {

	private Config config;
	private Scope scope;
	ConstraintToggler constraintToggler = new ConstraintToggler();
	private static Logger logger = Logger.getLogger(TableTruncater.class
			.getName());

	public TableTruncater(Config config, Scope scope) {
		this.config = config;
		this.scope = scope;
	}

	public void truncateAllDestinationTables() throws SQLException {
		Connection connection = DatabaseConnector.connect(config.destinationDB);
		List<Constraint> constraints = constraintToggler.disableConstraints(
				connection, config, scope);
		try {
			for (String tableName : scope.tables) {
				String qualifiedTableName = SQLHelper.qualifiedTableName(
						config.schemaName, tableName);
				System.out.println("Truncating " + tableName);
				truncateTable(qualifiedTableName, connection);
			}
		} finally {
			ConstraintToggler.enableConstraints(constraints, connection);
		}
	}

	public static void main(String[] args) throws MalformedException {
		if (args.length != 2) {
			System.out.println("Usage: java " + TableTruncater.class.getName()
					+ " config-file scope-file");
			return;
		}
		try {
			try {
				TableTruncater truncater = new TableTruncater(
						Config.fromFile(args[0]), Scope.fromFile(args[1]));
				truncater.truncateAllDestinationTables();
			} catch (DependantWithoutRuleException e) {
				// irrelevant, only need destination database coordinates
			} catch (IOException e) {
				System.err.println("Could not read file: " + e.getMessage());
				e.printStackTrace();
				return;
			}
		} catch (SQLException e) {
			System.err.println("Database error: " + e.getMessage());
			e.printStackTrace();
			return;
		}
	}

	public static void truncateTable(String qualifiedTableName,
			Connection connection) {
		try (Statement truncateStatement = connection.createStatement()) {
			truncateStatement.executeUpdate(SQLHelper.truncateTable(connection,
					qualifiedTableName));
			connection.commit();
		} catch (SQLException e) {
			logger.warning("Could not truncate table " + qualifiedTableName
					+ ": " + e.getMessage());
			logger.warning("A non-empty destination database "
					+ "can lead to subsequent errors.");
		}
	}

}
