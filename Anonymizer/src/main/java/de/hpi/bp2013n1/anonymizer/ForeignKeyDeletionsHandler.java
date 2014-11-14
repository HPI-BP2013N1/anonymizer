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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class ForeignKeyDeletionsHandler {
	
	/** table --> PK */
	Map<String, PrimaryKey> primaryKeys  = new HashMap<>();
	/** child table --> FKs */
	Multimap<String, ForeignKey> dependencies = ArrayListMultimap.create();
	Set<String> tablesWithDependants = new HashSet<>();
	/** table --> deleted PK tuples */
	Multimap<String, Map<String, Object>> deletedRows = HashMultimap.create();
	private Connection database;
	
	public void determineForeignKeysAmongTables(Connection database,
			String schema, Collection<String> tables) throws SQLException {
		this.database = database;
		DatabaseMetaData metaData = database.getMetaData();
		for (String table : tables) {
			primaryKeys.put(table, getPrimaryKey(database, schema, table));
			try (ResultSet importedKeys = metaData.getImportedKeys(
					database.getCatalog(), schema, table)) {
				String lastFK = null;
				ForeignKey fk = null;
				while (importedKeys.next()) {
					String parentTable = importedKeys.getString("PKTABLE_NAME");
					if (!tables.contains(parentTable))
						continue;
					tablesWithDependants.add(parentTable);
					String fkName = importedKeys.getString("FK_NAME");
					String fkNameAndTable = fkName + "." + parentTable;
					String parentColumn = importedKeys.getString("PKCOLUMN_NAME");
					String referencingColumn = importedKeys.getString("FKCOLUMN_NAME");
					PrimaryKey referencedPK = getPrimaryKey(database, schema,
							parentTable);
					if (!fkNameAndTable.equals(lastFK)) {
						fk = new ForeignKey(parentTable, referencedPK);
						dependencies.put(table, fk);
						lastFK = fkNameAndTable;
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
		if (!tablesWithDependants.contains(table))
			return;
		PrimaryKey pk = getPrimaryKey(database, deletedRow.getCurrentSchema(), table);
		// if memory is still a problem, save rows in a local h2 database backed by a file with a large cache depending on the heap size
		deletedRows.put(table, pk.keyValues(deletedRow));
		// how to detect if the parent row is gone if not the full PK is referenced?
		// if the delete strategy was used with a column to which a relation exists, 
		// deleting the dependent tuple would be desirable
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

	public void addForeignKeysForRuleDependents(Collection<Rule> rules) {
		for (Rule rule : rules) {
			addForeignKeyForRuleDependents(rule);
		}
		Multimap<String, ForeignKey> composedForeignKeys = 
				new ComposedForeignKeyDetector(dependencies, primaryKeys).composeForeignKeys();
		dependencies.putAll(composedForeignKeys);
	}

	private void addForeignKeyForRuleDependents(Rule rule) {
		for (TableField dependentField : rule.dependants) {
			Collection<ForeignKey> fks = dependencies.get(dependentField.table);
			if (isColumnPartOfForeignKey(dependentField, fks))
				continue;
			addArtificialForeignKey(rule, dependentField);
		}
	}

	private boolean isColumnPartOfForeignKey(TableField dependentField,
			Collection<ForeignKey> fks) {
		for (ForeignKey fk : fks) {
			if (fk.foreignKeyColumns.values().contains(dependentField.column))
				return true; // this field is taken care of already
		}
		return false;
	}

	private void addArtificialForeignKey(Rule rule, TableField dependentField) {
		ForeignKey newFK = new ForeignKey(rule.tableField.table, 
				new PrimaryKey(Lists.newArrayList(rule.tableField.column),
						Lists.newArrayList((String) null)));
		newFK.addForeignKeyColumn(rule.tableField.column, dependentField.column);
		dependencies.put(dependentField.table, newFK);
		tablesWithDependants.add(rule.tableField.table);
	}

}
