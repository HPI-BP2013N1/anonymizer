package de.hpi.bp2013n1.anonymizer.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/*
 * #%L
 * AnonymizerShared
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


public class ColumnDatatypeDescription {
	public String typename;
	public int length;

	public ColumnDatatypeDescription(String typename, int length) {
		super();
		this.typename = typename;
		this.length = length;
	}

	@Override
	public String toString() {
		switch (typename) {
		case "CHAR":
			return (typename + "(" + length + ")");
		case "CHARACTER":
			return (typename + "(" + length + ")");
		case "VARCHAR":
			return typename + "(" + length + ")";
		default:
			return typename;
		}
	}

	public static ColumnDatatypeDescription fromMetaData(
			TableField tableField, Connection connection) 
					throws SQLException {
		try (PreparedStatement selectColumnStatement = connection.prepareStatement(
				"SELECT " + tableField.column 
				+ " FROM " + tableField.schemaTable()
				+ " WHERE 1 = 0")) {
			ResultSetMetaData metadata = selectColumnStatement.getMetaData();
			// TODO: consider using getColumnType with java.sql.Types
			String typename = metadata.getColumnTypeName(1);
			int length = metadata.getColumnDisplaySize(1);
			return new ColumnDatatypeDescription(typename, length);
		}
	}
}
