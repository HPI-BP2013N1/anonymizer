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


import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.h2.tools.RunScript;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.CombinableMatcher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.ForeignKeyDeletionsHandler.ForeignKey;

public class ForeignKeyDeletionsHandlerTest {

	private ForeignKeyDeletionsHandler sut;

	@Before
	public void setUp() throws Exception {
		sut = new ForeignKeyDeletionsHandler();
	}
	
	@Test
	public void testDeletionInteraction() throws SQLException {
		// prepare some relationships
		PrimaryKey table1PK = new PrimaryKey();
		String parentColumnName = "A";
		table1PK.columnNames = Lists.newArrayList(parentColumnName);
		String parentTable = "TABLE1";
		sut.primaryKeys.put(parentTable, table1PK);
		ForeignKey foreignKey = sut.new ForeignKey(parentTable, table1PK);
		String referencingColumnName = "REF_A";
		foreignKey.addForeignKeyColumn(parentColumnName, referencingColumnName);
		String dependentTable = "TABLE2";
		sut.dependencies.put(dependentTable, foreignKey);
		sut.tablesWithDependants.add(parentTable);
		// prepare some rows
		int deletedPKValue = 0;
		int otherPKValue = 1;
		ResultSetRowReader deletedRow = mock(ResultSetRowReader.class);
		when(deletedRow.getCurrentTable()).thenReturn(parentTable);
		when(deletedRow.getObject(parentColumnName)).thenReturn(deletedPKValue);
		ResultSetRowReader dependentRow = mock(ResultSetRowReader.class);
		when(dependentRow.getCurrentTable()).thenReturn(dependentTable);
		when(dependentRow.getObject(referencingColumnName)).thenReturn(deletedPKValue);
		ResultSetRowReader independentRow = mock(ResultSetRowReader.class);
		when(independentRow.getCurrentTable()).thenReturn("someothertable");
		when(independentRow.getObject(referencingColumnName)).thenReturn(deletedPKValue);
		ResultSetRowReader independentRowInSameTable = mock(ResultSetRowReader.class);
		when(independentRowInSameTable.getCurrentTable()).thenReturn(dependentTable);
		when(independentRowInSameTable.getObject(referencingColumnName)).thenReturn(otherPKValue);
		
		sut.rowHasBeenDeleted(deletedRow);
		assertThat(sut.hasParentRowBeenDeleted(dependentRow), is(true));
		assertThat(sut.hasParentRowBeenDeleted(independentRow), is(false));
		assertThat(sut.hasParentRowBeenDeleted(independentRowInSameTable), is(false));
	}

	@Test
	public void testComposedKeyDeletionInteraction() throws SQLException {
		// prepare some relationships
		PrimaryKey table1PK = new PrimaryKey();
		String parentColumnName1 = "A";
		String parentColumnName2 = "B";
		table1PK.columnNames = Lists.newArrayList(parentColumnName1, parentColumnName2);
		String parentTable = "TABLE1";
		sut.primaryKeys.put(parentTable, table1PK);
		ForeignKey foreignKey = sut.new ForeignKey(parentTable, table1PK);
		String referencingColumnName1 = "REF_A";
		String referencingColumnName2 = "REF_B";
		foreignKey.addForeignKeyColumn(parentColumnName1, referencingColumnName1);
		foreignKey.addForeignKeyColumn(parentColumnName2, referencingColumnName2);
		String dependentTable = "TABLE2";
		sut.dependencies.put(dependentTable, foreignKey);
		sut.tablesWithDependants.add(parentTable);
		// prepare some rows
		int deletedPKValue1 = 0;
		int deletedPKValue2 = 1;
		int otherPKValue1 = 1;
		int otherPKValue2 = 0;
		ResultSetRowReader deletedRow = mock(ResultSetRowReader.class);
		when(deletedRow.getCurrentTable()).thenReturn(parentTable);
		when(deletedRow.getObject(parentColumnName1)).thenReturn(deletedPKValue1);
		when(deletedRow.getObject(parentColumnName2)).thenReturn(deletedPKValue2);
		ResultSetRowReader dependentRow = mock(ResultSetRowReader.class);
		when(dependentRow.getCurrentTable()).thenReturn(dependentTable);
		when(dependentRow.getObject(referencingColumnName1)).thenReturn(deletedPKValue1);
		when(dependentRow.getObject(referencingColumnName2)).thenReturn(deletedPKValue2);
		ResultSetRowReader independentRow = mock(ResultSetRowReader.class);
		when(independentRow.getCurrentTable()).thenReturn("someothertable");
		when(independentRow.getObject(referencingColumnName1)).thenReturn(deletedPKValue1);
		when(independentRow.getObject(referencingColumnName2)).thenReturn(deletedPKValue2);
		ResultSetRowReader independentRowInSameTable1 = mock(ResultSetRowReader.class);
		when(independentRowInSameTable1.getCurrentTable()).thenReturn(dependentTable);
		when(independentRowInSameTable1.getObject(referencingColumnName1)).thenReturn(otherPKValue1);
		when(independentRowInSameTable1.getObject(referencingColumnName2)).thenReturn(otherPKValue2);
		ResultSetRowReader independentRowInSameTable2 = mock(ResultSetRowReader.class);
		when(independentRowInSameTable2.getCurrentTable()).thenReturn(dependentTable);
		when(independentRowInSameTable2.getObject(referencingColumnName1)).thenReturn(otherPKValue1);
		when(independentRowInSameTable2.getObject(referencingColumnName2)).thenReturn(deletedPKValue2);
		ResultSetRowReader independentRowInSameTable3 = mock(ResultSetRowReader.class);
		when(independentRowInSameTable3.getCurrentTable()).thenReturn(dependentTable);
		when(independentRowInSameTable3.getObject(referencingColumnName1)).thenReturn(deletedPKValue1);
		when(independentRowInSameTable3.getObject(referencingColumnName2)).thenReturn(otherPKValue2);
		
		sut.rowHasBeenDeleted(deletedRow);
		assertThat(sut.hasParentRowBeenDeleted(dependentRow), is(true));
		assertThat(sut.hasParentRowBeenDeleted(independentRow), is(false));
		assertThat(sut.hasParentRowBeenDeleted(independentRowInSameTable1), is(false));
		assertThat(sut.hasParentRowBeenDeleted(independentRowInSameTable2), is(false));
		assertThat(sut.hasParentRowBeenDeleted(independentRowInSameTable3), is(false));
	}
	

	public static class PrimaryKeyMatcher extends TypeSafeMatcher<PrimaryKey> {
		private Matcher<Iterable<? extends String>> columnNamesMatcher;

		public PrimaryKeyMatcher(String... columnNames) {
			this.columnNamesMatcher = contains(columnNames);
		}
		
		@Override
		public void describeTo(Description description) {
			description.appendText("PK(")
			.appendDescriptionOf(columnNamesMatcher)
			.appendText(")");
		}

		@Override
		protected boolean matchesSafely(PrimaryKey item) {
			return columnNamesMatcher.matches(item.columnNames);
		}
	}
	
	@Factory
	public Matcher<PrimaryKey> primaryKey(String... columns) {
		return new PrimaryKeyMatcher(columns);
	}
	
	public static class ForeignKeyMatcher extends TypeSafeMatcher<ForeignKey> {
		private Matcher<String> parentTableMatcher;
		private Matcher<PrimaryKey> pkMatcher;
		private Matcher<Map<? extends String,? extends String>> fkColumnsMatcher;

		public ForeignKeyMatcher(String parentTable, Matcher<PrimaryKey> pk,
				String... pkAndFkColumns) {
			Preconditions.checkArgument(pkAndFkColumns.length % 2 == 0, 
					"need even number of PK and FK column names");
			this.parentTableMatcher = is(parentTable);
			this.pkMatcher = pk;
			CombinableMatcher<Map<? extends String,? extends String>> conjunction = 
					new CombinableMatcher<>(
							hasEntry(pkAndFkColumns[0], pkAndFkColumns[1]));
			for (int i = 2; i < pkAndFkColumns.length; i += 2) {
				conjunction = conjunction.and(hasEntry(pkAndFkColumns[i], pkAndFkColumns[i+1]));
			}
			fkColumnsMatcher = conjunction;
		}
		
		@Override
		public void describeTo(Description description) {
			description.appendText("Foreign key whose parent ")
			.appendDescriptionOf(parentTableMatcher)
			.appendText(" with ")
			.appendDescriptionOf(pkMatcher)
			.appendText(" and ")
			.appendDescriptionOf(fkColumnsMatcher)
			.appendText(" as foreign key column mapping");
		}

		@Override
		protected boolean matchesSafely(ForeignKey fk) {
			return parentTableMatcher.matches(fk.parentTable)
					&& pkMatcher.matches(fk.parentPrimaryKey)
					&& fkColumnsMatcher.matches(fk.foreignKeyColumns);
		}
	}
	
	@Factory
	public Matcher<ForeignKey> foreignKey(String parentTable, Matcher<PrimaryKey> parentPK, 
			String... pkAndFkColumns) {
		return new ForeignKeyMatcher(parentTable, parentPK, pkAndFkColumns);
	}
	
	@Test
	public void testRelationshipDetection() throws SQLException {
		Connection db = DriverManager.getConnection("jdbc:h2:mem:");
		String ddl = "CREATE TABLE TABLE1 (A INT PRIMARY KEY);\n"
				+ "CREATE TABLE TABLE2 (REF_A INT PRIMARY KEY, "
				+ "FOREIGN KEY (REF_A) REFERENCES TABLE1(A))";
		try (StringReader ddlReader = new StringReader(ddl)) {
			RunScript.execute(db, ddlReader);
		}
		sut.determineForeignKeysAmongTables(db, "PUBLIC", 
				Lists.newArrayList("TABLE1", "TABLE2"));
		assertThat(sut.primaryKeys, hasEntry(is("TABLE1"), primaryKey("A")));
		assertThat(sut.dependencies.asMap(), hasEntry(is("TABLE2"), 
				contains(foreignKey("TABLE1", primaryKey("A"), "A", "REF_A"))));
	}
}
