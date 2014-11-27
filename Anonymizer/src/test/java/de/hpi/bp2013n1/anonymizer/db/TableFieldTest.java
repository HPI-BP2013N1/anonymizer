package de.hpi.bp2013n1.anonymizer.db;

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

import org.junit.Test;

public class TableFieldTest {

	@Test
	public void testTableFieldStringString() {
		TableField sut = new TableField("table.column", "schema");
		assertThat(sut.column, is("column"));
		assertThat(sut.table, is("table"));
		assertThat(sut.schema, is("schema"));
		sut = new TableField("table", "schema");
		assertThat(sut.column, nullValue());
		assertThat(sut.table, is("table"));
		assertThat(sut.schema, is("schema"));
	}

	@Test
	public void testSchemaTable() {
		TableField sut = new TableField("table.column", "schema");
		assertThat(sut.schemaTable(), is("schema.table"));
	}
	
	@Test
	public void testStringOnlyConstructor() {
		TableField sut = new TableField("A.B.C");
		assertThat(sut.schema, is("A"));
		assertThat(sut.table, is("B"));
		assertThat(sut.column, is("C"));
		sut = new TableField("A.B");
		assertThat(sut.table, is("A"));
		assertThat(sut.column, is("B"));
		sut = new TableField("A");
		assertThat(sut.table, is("A"));
		assertThat(sut.column, is(nullValue()));
	}
}
