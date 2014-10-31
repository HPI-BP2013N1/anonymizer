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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.RowRetainService.InsertRetainMarkFailed;
import de.hpi.bp2013n1.anonymizer.shared.Config;

public class RowRetainServiceTest {

	private RowRetainService sut;

	@Before
	public void setUp() throws Exception {
		sut = new RowRetainService(null, null);
	}

	@Test
	public void testRetainInsertQuery() {
		PrimaryKey pk = new PrimaryKey();
		pk.columnNames = Lists.newArrayList("A");
		assertThat(sut.retainInsertQuery("S", "T", pk),
				equalTo("INSERT INTO S.T_RETAINED (A) VALUES (?)"));
		pk.columnNames = Lists.newArrayList("A", "B");
		assertThat(sut.retainInsertQuery("S", "T", pk),
				equalTo("INSERT INTO S.T_RETAINED (A,B) VALUES (?,?)"));
	}

	@Test @Ignore("not implemented yet")
	public void testRetainInsertQueryEscaped() {
		PrimaryKey pk = new PrimaryKey();
		pk.columnNames = Lists.newArrayList("A");
		assertThat(sut.retainInsertQuery("Schema", "Table", pk),
				equalTo("INSERT INTO \"Schema\".\"Table\" (A) VALUES (?)"));
		assertThat(sut.retainInsertQuery("A SCHEMA", "A TABLE", pk),
				equalTo("INSERT INTO \"A SCHEMA\".\"A TABLE\" (A) VALUES (?)"));
	}
	
	@Test
	public void testRetainCurrentRow() throws ClassNotFoundException, IOException, SQLException, InsertRetainMarkFailed {
		Config config = StandardTestDataFixture.makeStubConfig();
		try (TestDataFixture stub = new StandardTestDataFixture(config, null)) {
			sut = new RowRetainService(stub.originalDbConnection, 
					stub.transformationDbConnection);
			org.h2.tools.RunScript.execute(stub.originalDbConnection, 
					new StringReader("CREATE SCHEMA S; "
							+ "CREATE TABLE S.T (A VARCHAR(20) PRIMARY KEY); "
							+ "CREATE TABLE S.T2 (A VARCHAR(20) PRIMARY KEY)"));
			ResultSet rowS = mock(ResultSet.class, RETURNS_SMART_NULLS);
			when(rowS.getObject(anyString())).thenReturn("test");
			ResultSetRowReader row = new ResultSetRowReader(rowS);
			ResultSet otherRowS = mock(ResultSet.class, RETURNS_SMART_NULLS);
			when(otherRowS.getObject(anyString())).thenReturn("something else");
			ResultSetRowReader otherRow = new ResultSetRowReader(otherRowS);
			sut.retainCurrentRow("S", "T", row);
			assertThat(sut.currentRowShouldBeRetained("S", "T", row), is(true));
			assertThat(sut.currentRowShouldBeRetained("S", "T2", row), is(false));
			assertThat(sut.currentRowShouldBeRetained("S", "T", otherRow), is(false));

			sut = new RowRetainService(stub.originalDbConnection, 
					stub.transformationDbConnection);
			assertThat("Retain mark should be persistent in the database",
					sut.currentRowShouldBeRetained("S", "T", row), is(true));
		}
	}
	
	@Test
	public void testSelectRetainedPrimaryKeyQuery() {
		Map<String, Object> comparisons = new TreeMap<>();
		comparisons.put("A = ?", true);
		assertThat(sut.selectRetainedPrimaryKeyQuery("S", "T", comparisons),
				equalTo("SELECT 1 FROM S.T_RETAINED WHERE A = ?"));
		comparisons.put("B = ?", true);
		assertThat(sut.selectRetainedPrimaryKeyQuery("S", "T", comparisons),
				equalTo("SELECT 1 FROM S.T_RETAINED WHERE A = ? AND B = ?"));
	}
}
