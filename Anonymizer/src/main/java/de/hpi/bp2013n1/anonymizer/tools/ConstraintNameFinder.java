package de.hpi.bp2013n1.anonymizer.tools;

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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import de.hpi.bp2013n1.anonymizer.Constraint;
import de.hpi.bp2013n1.anonymizer.db.TableField;

public class ConstraintNameFinder {

	private Connection connection;

	public ConstraintNameFinder(Connection connection) {
		this.connection = connection;
	}

	public ArrayList<Constraint> findConstraintName(TableField tableAlias) {
		return findConstraintName(tableAlias.table);
	}

	public ArrayList<Constraint> findConstraintName(String tableName) {
		ArrayList<Constraint> result = new ArrayList<Constraint>();
		try {
			DatabaseMetaData metaData = connection.getMetaData();
			try (ResultSet resultSet = metaData.getExportedKeys(
					connection.getCatalog(), null, tableName)) {
				while (resultSet.next())
					result.add(new Constraint(
							resultSet.getString("FKTABLE_SCHEM"), 
							resultSet.getString("FKTABLE_NAME"), 
							resultSet.getString("FK_NAME")));
			}
			// TODO: unit test this against DB2
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}

}
