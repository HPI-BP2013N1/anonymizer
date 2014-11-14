package de.hpi.bp2013n1.anonymizer;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ForeignKeyBuilder {
	
	String parentTable;
	PrimaryKey pk = null;
	Map<String, String> parentColumnsByChildColumns = Maps.newLinkedHashMap();
	
	public ForeignKeyBuilder(String parentTable) {
		this.parentTable = parentTable;
	}

	public ForeignKeyBuilder(String parentTable, PrimaryKey pk) {
		this.parentTable = parentTable;
		this.pk = pk;
	}

	public static ForeignKeyBuilder withParent(String parentTable) {
		return new ForeignKeyBuilder(parentTable);
	}

	public static ForeignKeyBuilder withParent(String parentTable, PrimaryKey pk) {
		return new ForeignKeyBuilder(parentTable, pk);
	}
	
	public class ReferenceConstruction {
		String childColumn;
		
		public ReferenceConstruction(String childColumn) {
			this.childColumn = childColumn;
		}
		
		public ForeignKeyBuilder to(String parentColumn) {
			parentColumnsByChildColumns.put(childColumn, parentColumn);
			return ForeignKeyBuilder.this;
		}
	}
	
	public ReferenceConstruction referenceFrom(String parentColumn) {
		return new ReferenceConstruction(parentColumn);
	}
	
	public ForeignKey build() {
		if (pk == null) {
			pk = new PrimaryKey(Lists.newArrayList(parentColumnsByChildColumns.values()));
		}
		ForeignKey foreignKey = new ForeignKey(parentTable, pk);
		for (Map.Entry<String, String> pair : parentColumnsByChildColumns.entrySet()) {
			foreignKey.addForeignKeyColumn(pair.getValue(), pair.getKey());
		}
		return foreignKey;
	}
}
