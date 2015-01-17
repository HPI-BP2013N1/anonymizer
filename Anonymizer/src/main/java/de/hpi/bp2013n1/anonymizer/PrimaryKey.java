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


import static com.google.common.base.Preconditions.checkArgument;

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

class PrimaryKey {
	List<String> columnNames;
	List<String> columnTypeNames;
	List<Boolean> columnNullable;
	String keyName;
	
	public PrimaryKey() {
		// let the caller fill it
	}

	public PrimaryKey(List<String> columnNames, List<String> columnTypeNames,
			List<Boolean> columnsNullable) {
		checkArgument(columnNames.size() == columnTypeNames.size());
		checkArgument(columnNames.size() == columnsNullable.size());
		this.columnNames = columnNames;
		this.columnTypeNames = columnTypeNames;
		this.columnNullable = columnsNullable;
	}

	public PrimaryKey(List<String> columnNames, List<String> columnTypeNames) {
		this.columnNames = columnNames;
		this.columnTypeNames = columnTypeNames;
		this.columnNullable = Lists.newArrayList();
		for (@SuppressWarnings("unused") String _ : columnNames) {
			columnNullable.add(false);
		}
	}

	public PrimaryKey(List<String> columnNames) {
		this.columnNames = columnNames;
		this.columnTypeNames = Lists.newArrayList();
		this.columnNullable = Lists.newArrayList();
		for (@SuppressWarnings("unused") String _ : columnNames) {
			columnTypeNames.add(null);
			columnNullable.add(false);
		}
	}

	public PrimaryKey(String... columnNames) {
		this(Lists.newArrayList(columnNames));
	}

	public PrimaryKey(String schema, String table, Connection database) throws SQLException {
		DatabaseMetaData metaData = database.getMetaData();
		try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, schema, table)) {
			if (!pkResultSet.next()) {
				useAllColumnsAsPrimaryKey(schema, table, database);
				return;
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
				columnNullable = Lists.newArrayListWithExpectedSize(columnNames.size());
				for (int i = 1; i <= columnNames.size(); i++) {
					columnTypeNames.add(selectPKMetaData.getColumnTypeName(i));
					columnNullable.add(selectPKMetaData.isNullable(i)
							== ResultSetMetaData.columnNullable);
				}
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder("(");
		for (String column : this.columnNames) {
			stringBuilder.append(column).append(",");
		}
		stringBuilder.setCharAt(stringBuilder.length() - 1, ')');
		return stringBuilder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnNames == null) ? 0 : columnNames.hashCode());
		result = prime * result
				+ ((columnTypeNames == null) ? 0 : columnTypeNames.hashCode());
		result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PrimaryKey other = (PrimaryKey) obj;
		if (columnNames == null) {
			if (other.columnNames != null)
				return false;
		} else if (!columnNames.equals(other.columnNames))
			return false;
		if (columnTypeNames == null) {
			if (other.columnTypeNames != null)
				return false;
		} else if (!columnTypeNames.equals(other.columnTypeNames))
			return false;
		if (keyName == null) {
			if (other.keyName != null)
				return false;
		} else if (!keyName.equals(other.keyName))
			return false;
		return true;
	}

	private void useAllColumnsAsPrimaryKey(String schema, String table,
			Connection database) throws SQLException {
		try (PreparedStatement selectColumnStatement = database.prepareStatement(
				"SELECT *  FROM " + schema + "." + table + " WHERE 1 = 0")) {
			ResultSetMetaData metadata = selectColumnStatement.getMetaData();
			int columnCount = metadata.getColumnCount();
			columnNames = new ArrayList<>(columnCount);
			columnTypeNames = new ArrayList<>(columnCount);
			columnNullable = new ArrayList<>(columnCount);
			// TODO: consider using getColumnType with java.sql.Types
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(metadata.getColumnName(i));
				columnTypeNames.add(metadata.getColumnTypeName(i));
				columnNullable.add(metadata.isNullable(i)
						== ResultSetMetaData.columnNullable);
			}
		}
	}

	public List<String> columnNames() {
		return ImmutableList.copyOf(columnNames);
	}
	
	public List<String> columnDefinitions() {
		ArrayList<String> definitions = new ArrayList<>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++)
			definitions.add(columnDefinition(i));
		return definitions;
	}

	private String columnDefinition(int columnIndex) {
		StringBuilder builder = new StringBuilder();
		builder.append(columnNames.get(columnIndex)).append(" ");
		builder.append(columnTypeNames.get(columnIndex));
		if (!columnNullable.get(columnIndex))
			builder.append(" NOT NULL");
		return builder.toString();
	}

	public Map<String, Object> keyValues(ResultSetRowReader row)
			throws SQLException {
		Map<String, Object> comparisons = new TreeMap<>();
		for (String pkColumn : columnNames)
			comparisons.put(pkColumn, row.getObject(pkColumn));
		return comparisons;
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