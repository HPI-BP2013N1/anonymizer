package de.hpi.bp2013n1.anonymizer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class ComposedForeignKeyDetector {
	
	private Multimap<String, ForeignKey> currentForeignKeysByChildTable;
	private Map<String, PrimaryKey> primaryKeys;

	public ComposedForeignKeyDetector(
			Multimap<String, ForeignKey> currentForeignKeysByChildTable,
			Map<String, PrimaryKey> primaryKeys) {
		this.currentForeignKeysByChildTable = currentForeignKeysByChildTable;
		this.primaryKeys = primaryKeys;
	}

	public Multimap<String, ForeignKey> composeForeignKeys() {
		Multimap<String, ForeignKey> newForeignKeys = ArrayListMultimap.create();
		for (String childTable : currentForeignKeysByChildTable.keySet()) {
			Collection<ForeignKey> currentForeignKeys = currentForeignKeysByChildTable.get(childTable);
			Multimap<String, ForeignKey> fksByParentTable = 
					groupKeysByParentTable(currentForeignKeys);
			for (String parentTable : fksByParentTable.keySet()) {
				PrimaryKey parentPK = primaryKeys.get(parentTable);
				if (parentPK.columnNames.size() <= 1)
					continue; // no composed primary key
				Multimap<String, String> foundReferences = ArrayListMultimap.create();
				for (ForeignKey fk : fksByParentTable.get(parentTable)) {
					if (fk.foreignKeyColumns.size() > 1)
						continue; // already composed foreign key
					String parentColumn = fk.parentPrimaryKey.columnNames.get(0);
					if (!parentPK.columnNames.contains(parentColumn))
						continue; // not referencing primary key
					foundReferences.put(parentColumn,
							fk.foreignKeyColumns.get(parentColumn));
				}
				buildForeignKeys(parentTable, parentPK, childTable, foundReferences, newForeignKeys);
			}
		}
		return newForeignKeys;
	}

	Multimap<String, ForeignKey> groupKeysByParentTable(
			Collection<ForeignKey> currentForeignKeys) {
		Multimap<String, ForeignKey> fkByParentTable = ArrayListMultimap.create();
		for (ForeignKey fk : currentForeignKeys) {
			fkByParentTable.put(fk.parentTable, fk);
		}
		return fkByParentTable;
	}

	void buildForeignKeys(String parentTable, PrimaryKey parentPK,
			String childTable, Multimap<String, String> foundReferences,
			Multimap<String, ForeignKey> newForeignKeys) {
		Set<String> foundReferencesParentColumns = foundReferences.keySet();
		if (foundReferencesParentColumns.equals(
				Sets.newTreeSet(parentPK.columnNames))) {
			buildForeignKeys(parentTable, parentPK, childTable, foundReferences,
					Lists.newArrayList(foundReferencesParentColumns), 
					new TreeMap<String, String>(), newForeignKeys);
		} // else no combination would reference the primary key
	}

	void buildForeignKeys(String parentTable,
			PrimaryKey parentPK, String childTable, 
			Multimap<String, String> references,
			List<String> parentColumns, Map<String, String> currentColumnMapping, Multimap<String, ForeignKey> newForeignKeys) {
		if (parentColumns.size() < 1) {
			ForeignKey newFK = new ForeignKey(parentTable, parentPK);
			for (Map.Entry<String, String> pair : currentColumnMapping.entrySet()) {
				newFK.addForeignKeyColumn(pair.getKey(), pair.getValue());
			}
			newForeignKeys.put(childTable, newFK);
			return;
		}
		String thisRecursionsParentColumn = parentColumns.remove(0);
		try {
			for (String column : references.get(thisRecursionsParentColumn)) {
				currentColumnMapping.put(thisRecursionsParentColumn, column);
				// recursion with one column less in parentColumns
				buildForeignKeys(parentTable, parentPK, childTable, references,
						parentColumns, currentColumnMapping, newForeignKeys);
			}
		} finally {
			currentColumnMapping.remove(thisRecursionsParentColumn);
			parentColumns.add(0, thisRecursionsParentColumn);
		}
	}

}
