package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class ComposedForeignKeyDetectorTest {
	
	private List<String> makeNTypes(int n) {
		List<String> out = Lists.newArrayList();
		for (int i = 0; i < n; i++)
			out.add("SMALLINT");
		return out;
	}
	
	private PrimaryKey makePK(String... columnNames) {
		return new PrimaryKey(Lists.newArrayList(columnNames),
				makeNTypes(columnNames.length));
	}

	@Test
	public void testBuildForeignKeys() {
		Map<String, PrimaryKey> primaryKeys = Maps.newHashMap();
		String parentTable = "parent";
		String childTable = "child";
		PrimaryKey parentPK = makePK("a", "b");
		primaryKeys.put(parentTable, parentPK);
		primaryKeys.put(childTable, makePK("id"));
		Multimap<String, ForeignKey> currentForeignKeysByChildTable = ArrayListMultimap.create();
		currentForeignKeysByChildTable.put(childTable, 
				ForeignKeyBuilder.withParent(parentTable)
				.referenceFrom("a").to("a").build());
		currentForeignKeysByChildTable.put(childTable, 
				ForeignKeyBuilder.withParent(parentTable)
				.referenceFrom("b").to("b").build());
		ComposedForeignKeyDetector sut = new ComposedForeignKeyDetector(
				currentForeignKeysByChildTable, primaryKeys);
		Multimap<String, ForeignKey> newForeignKeys = ArrayListMultimap.create();
		Multimap<String, String> foundReferences = ArrayListMultimap.create();
		
		foundReferences.put("a", "a");
		foundReferences.put("b", "b");
		sut.buildForeignKeys(parentTable, parentPK, childTable, foundReferences, newForeignKeys);
		ForeignKey expectedFK1 = ForeignKeyBuilder.withParent(parentTable, parentPK)
				.referenceFrom("a").to("a")
				.referenceFrom("b").to("b").build();
		assertThat(newForeignKeys.asMap(),
				hasEntry(is(childTable), contains(expectedFK1)));
		
		foundReferences.put("a", "c");
		newForeignKeys.clear();
		ForeignKey expectedFK2 = ForeignKeyBuilder.withParent(parentTable, parentPK)
				.referenceFrom("c").to("a")
				.referenceFrom("b").to("b").build();
		sut.buildForeignKeys(parentTable, parentPK, childTable, foundReferences, newForeignKeys);
		assertThat(newForeignKeys.asMap(), 
				hasEntry(is(childTable), containsInAnyOrder(expectedFK1, expectedFK2)));

		foundReferences.put("b", "d");
		newForeignKeys.clear();
		ForeignKey expectedFK3 = ForeignKeyBuilder.withParent(parentTable, parentPK)
				.referenceFrom("a").to("a")
				.referenceFrom("d").to("b").build();
		ForeignKey expectedFK4 = ForeignKeyBuilder.withParent(parentTable, parentPK)
				.referenceFrom("c").to("a")
				.referenceFrom("d").to("b").build();
		sut.buildForeignKeys(parentTable, parentPK, childTable, foundReferences, newForeignKeys);
		assertThat(newForeignKeys.asMap(), 
				hasEntry(is(childTable), containsInAnyOrder(expectedFK1, expectedFK2, 
						expectedFK3, expectedFK4)));
	}
	
	@Test
	public void testComposeForeignKeys() {
		Map<String, PrimaryKey> primaryKeys = Maps.newHashMap();
		String parentTable1 = "parent1";
		String parentTable2 = "parent2";
		String childTable1 = "child1";
		String childTable2 = "child2";
		String childTable3 = "child3";
		PrimaryKey parent1PK = makePK("a", "b");
		PrimaryKey parent2PK = makePK("c", "d");
		primaryKeys.put(parentTable1, parent1PK);
		primaryKeys.put(parentTable2, parent2PK);
		primaryKeys.put(childTable1, makePK("id"));
		primaryKeys.put(childTable2, makePK("c", "d"));
		primaryKeys.put(childTable3, makePK("id"));
		Multimap<String, ForeignKey> currentForeignKeysByChildTable = ArrayListMultimap.create();
		// childTable1 Rule-derived relationships
		currentForeignKeysByChildTable.put(childTable1, 
				ForeignKeyBuilder.withParent(parentTable1)
				.referenceFrom("a").to("a").build());
		currentForeignKeysByChildTable.put(childTable1, 
				ForeignKeyBuilder.withParent(parentTable1)
				.referenceFrom("b").to("b").build());
		// childTable2 Rule-derived relationships
		currentForeignKeysByChildTable.put(childTable2, 
				ForeignKeyBuilder.withParent(parentTable1)
				.referenceFrom("a").to("a").build());
		currentForeignKeysByChildTable.put(childTable2, 
				ForeignKeyBuilder.withParent(parentTable1)
				.referenceFrom("b").to("b").build());
		currentForeignKeysByChildTable.put(childTable2,
				ForeignKeyBuilder.withParent(parentTable2)
				.referenceFrom("c").to("c").build());
		currentForeignKeysByChildTable.put(childTable2,
				ForeignKeyBuilder.withParent(parentTable2)
				.referenceFrom("d").to("d").build());
		// childTable3 Rule-derived relationship
		currentForeignKeysByChildTable.put(childTable3,
				ForeignKeyBuilder.withParent(parentTable1)
				.referenceFrom("a").to("a").build());
		ComposedForeignKeyDetector sut = new ComposedForeignKeyDetector(
				currentForeignKeysByChildTable, primaryKeys);
		
		Multimap<String, ForeignKey> result = sut.composeForeignKeys();
		assertThat(result.keySet(), containsInAnyOrder(childTable1, childTable2));
		assertThat(result.get(childTable1), 
				contains(ForeignKeyBuilder.withParent(parentTable1, parent1PK)
						.referenceFrom("a").to("a")
						.referenceFrom("b").to("b").build()));
		assertThat(result.get(childTable2), 
				containsInAnyOrder(ForeignKeyBuilder.withParent(parentTable1, parent1PK)
						.referenceFrom("a").to("a")
						.referenceFrom("b").to("b").build(),
						ForeignKeyBuilder.withParent(parentTable2, parent2PK)
						.referenceFrom("c").to("c")
						.referenceFrom("d").to("d").build()));
	}
}
