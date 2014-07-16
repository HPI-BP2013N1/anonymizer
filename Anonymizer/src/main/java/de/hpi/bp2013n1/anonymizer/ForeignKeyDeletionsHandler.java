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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ForeignKeyDeletionsHandler {
	
	class ForeignKey {
		String parentTable;
		PrimaryKey parentPrimaryKey;
		// keys = parent table column names, values = referencing column names
		Map<String, String> foreignKeyColumns = new TreeMap<>();
		
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
				comparisons.put(fkColumn.getKey(), row.getObject(fkColumn.getValue()));
			}
			return comparisons;
		}
	}
	
	Map<String, PrimaryKey> primaryKeys = new HashMap<>();
	Multimap<String, ForeignKey> dependencies = ArrayListMultimap.create();
	Multimap<String, Map<String, Object>> deletedRows = HashMultimap.create();
	private Connection database;
	
	public void determineForeignKeysAmongTables(Connection database,
			String schema, Collection<String> tables) throws SQLException {
		this.database = database;
		DatabaseMetaData metaData = database.getMetaData();
		for (String table : tables) {
			try (ResultSet importedKeys = metaData.getImportedKeys(null, schema, table)) {
				String previousFkName = null;
				ForeignKey fk = null;
				while (importedKeys.next()) {
					String parentTable = importedKeys.getString("PKTABLE_NAME");
					if (!tables.contains(parentTable))
						continue;
					String fkName = importedKeys.getString("FK_NAME");
					String parentColumn = importedKeys.getString("PKCOLUMN_NAME");
					String referencingColumn = importedKeys.getString("FKCOLUMN_NAME");
					PrimaryKey referencedPK = getPrimaryKey(database, schema,
							parentTable);
					if (!fkName.equals(previousFkName)) {
						fk = new ForeignKey(parentTable, referencedPK);
						dependencies.put(table, fk);
					}
					fk.addForeignKeyColumn(parentColumn, referencingColumn);
				}
			}
		}
	}

	private PrimaryKey getPrimaryKey(Connection database, String schema,
			String table) throws SQLException {
		PrimaryKey referencedPK = primaryKeys.get(table);
		if (referencedPK == null) {
			referencedPK = new PrimaryKey(schema, table, database);
			primaryKeys.put(table, referencedPK);
		}
		return referencedPK;
	}
	
	public void rowHasBeenDeleted(ResultSetRowReader deletedRow) throws SQLException {
		String table = deletedRow.getCurrentTable();
		PrimaryKey pk = getPrimaryKey(database, deletedRow.getCurrentSchema(), table);
		deletedRows.put(table, pk.keyValues(deletedRow));
	}
	
	public boolean hasParentRowBeenDeleted(ResultSetRowReader row) throws SQLException {
		String table = row.getCurrentTable();
		Collection<ForeignKey> rowDependencies = dependencies.get(table);
		for (ForeignKey fk : rowDependencies) {
			if (deletedRows.containsEntry(fk.parentTable, fk.referencedValues(row)))
				return true;
		}
		return false;
	}

}
