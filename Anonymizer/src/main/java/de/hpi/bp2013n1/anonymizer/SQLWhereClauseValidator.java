package de.hpi.bp2013n1.anonymizer;

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
			testSelect.execute("SELECT 1 FROM " + rule.getTableField().schemaTable()
					+ " WHERE " + rule.getAdditionalInfo());
		} catch (SQLException e) {
			Logger.getLogger(getClass().getName()).severe("Rule " + rule
					+ " has an invalid retain criterion: " + e.getMessage());
			return false;
		}
		return true;
	}

}
