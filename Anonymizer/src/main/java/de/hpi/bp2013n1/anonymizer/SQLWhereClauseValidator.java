package de.hpi.bp2013n1.anonymizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class SQLWhereClauseValidator {

	private Connection connection;

	public SQLWhereClauseValidator(Connection connection) {
		this.connection = connection;
	}

	boolean additionalInfoIsValidWhereClause(Rule rule) {
		try (Statement testSelect = connection.createStatement()) {
			testSelect.execute("SELECT 1 FROM " + rule.tableField.schemaTable()
					+ " WHERE " + rule.additionalInfo);
		} catch (SQLException e) {
			Logger.getLogger(getClass().getName()).severe("Rule " + rule 
					+ " has an invalid retain criterion: " + e.getMessage());
			return false;
		}
		return true;
	}

}
