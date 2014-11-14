package de.hpi.bp2013n1.anonymizer;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

class ForeignKey {
	String parentTable;
	PrimaryKey parentPrimaryKey;
	// keys = parent table column names, values = referencing column names
	Map<String, String> foreignKeyColumns = new TreeMap<>();
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((foreignKeyColumns == null) ? 0 : foreignKeyColumns
						.hashCode());
		result = prime
				* result
				+ ((parentPrimaryKey == null) ? 0 : parentPrimaryKey.hashCode());
		result = prime * result
				+ ((parentTable == null) ? 0 : parentTable.hashCode());
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
		ForeignKey other = (ForeignKey) obj;
		if (foreignKeyColumns == null) {
			if (other.foreignKeyColumns != null)
				return false;
		} else if (!foreignKeyColumns.equals(other.foreignKeyColumns))
			return false;
		if (parentPrimaryKey == null) {
			if (other.parentPrimaryKey != null)
				return false;
		} else if (!parentPrimaryKey.equals(other.parentPrimaryKey))
			return false;
		if (parentTable == null) {
			if (other.parentTable != null)
				return false;
		} else if (!parentTable.equals(other.parentTable))
			return false;
		return true;
	}

	public ForeignKey(String parentTable, PrimaryKey parentPrimaryKey) {
		this.parentTable = parentTable;
		this.parentPrimaryKey = parentPrimaryKey;
	}
	
	public void addForeignKeyColumn(String parentColumnName, String referencingColumnName) {
		foreignKeyColumns.put(parentColumnName, referencingColumnName);
	}

	public Map<String, Object> referencedValues(ResultSetRowReader row)
			throws SQLException {
		Map<String, Object> comparisons = new TreeMap<>();
		for (Map.Entry<String, String> fkColumn : foreignKeyColumns.entrySet()) {
			if (!parentPrimaryKey.columnNames.contains(fkColumn.getKey()))
				continue; // not a primary key column, so irrelevant for existence check
			comparisons.put(fkColumn.getKey(), row.getObject(fkColumn.getValue()));
		}
		return comparisons;
	}
}