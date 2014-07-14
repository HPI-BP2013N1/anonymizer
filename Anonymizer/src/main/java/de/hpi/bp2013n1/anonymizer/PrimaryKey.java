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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Joiner;

import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

class PrimaryKey {
	List<String> columnNames;
	List<String> columnTypeNames;
	String keyName;
	
	public PrimaryKey() {
		// let the caller fill it
	}
	
	public PrimaryKey(String schema, String table, Connection database) throws SQLException {
		DatabaseMetaData metaData = database.getMetaData();
		try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, schema, table)) {
			if (!pkResultSet.next()) {
				// TODO: no primary key
			}
			keyName = pkResultSet.getString("PK_NAME");
			TreeMap<Integer, String> columns = new TreeMap<>();
			do {
				columns.put(pkResultSet.getInt("KEY_SEQ"), 
						pkResultSet.getString("COLUMN_NAME"));
			} while (pkResultSet.next());
			columnNames = new ArrayList<>(columns.values());
			try (PreparedStatement select = database.prepareStatement(
					"SELECT " + Joiner.on(',').join(columnNames) 
					+ " FROM " + SQLHelper.qualifiedTableName(schema, table)
					+ " WHERE 1 = 0")) { // only interested in metadata
				ResultSetMetaData selectPKMetaData = select.getMetaData();
				columnTypeNames = new ArrayList<>(columnNames.size());
				for (int i = 1; i <= columnNames.size(); i++) {
					columnTypeNames.add(selectPKMetaData.getColumnTypeName(i));
				}
			}
		}
	}
	
	public List<String> columnDefinitions() {
		ArrayList<String> definitions = new ArrayList<>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++)
			definitions.add(columnNames.get(i) + " " 
					+ columnTypeNames.get(i));
		return definitions;
	}

	public Map<String, Object> whereComparisons(ResultSetRowReader row)
			throws SQLException {
		Map<String, Object> comparisons = new TreeMap<>();
		for (String pkColumn : columnNames)
			comparisons.put(pkColumn + " = ?", row.getObject(pkColumn));
		return comparisons;
	}
	
	public static String whereComparisonClause(Map<String, Object> comparisons) {
		return Joiner.on(" AND ").join(comparisons.keySet());
	}
	
	public String whereComparisonClause(ResultSetRowReader row) throws SQLException {
		return whereComparisonClause(whereComparisons(row));
	}

	static void setParametersForPKQuery(Map<String, Object> comparisons,
			PreparedStatement select) throws SQLException {
		int i = 1;
		for (Object value : comparisons.values()) {
			select.setObject(i++, value);
		}
	}
}