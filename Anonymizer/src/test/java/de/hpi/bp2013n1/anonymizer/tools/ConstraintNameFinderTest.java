package de.hpi.bp2013n1.anonymizer.tools;

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


import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

public class ConstraintNameFinderTest {

	private ConstraintNameFinder sut;
	private Connection connection;

	@Before
	public void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:");
		sut = new ConstraintNameFinder(connection);
	}

	@Test
	public void testFindConstraintName() throws SQLException {
		try (Statement ddl = connection.createStatement()) {
			ddl.executeUpdate("CREATE TABLE TABLE1 (A INT PRIMARY KEY)");
			ddl.executeUpdate("CREATE TABLE TABLE2 (A INT, B INT, PRIMARY KEY(A, B))");
			ddl.executeUpdate("CREATE TABLE TABLE3 (A INT, "
					+ "FOREIGN KEY(A) REFERENCES TABLE1(A))");
			ddl.executeUpdate("CREATE TABLE TABLE4 (A INT, B INT, "
					+ "FOREIGN KEY(A, B) REFERENCES TABLE2(A, B))");
			ddl.executeUpdate("CREATE TABLE TABLE5 (A INT, "
					+ "FOREIGN KEY(A) REFERENCES TABLE1(A))");
			ddl.executeUpdate("CREATE TABLE TABLE6 (A INT)");
		}
		assertThat(sut.findConstraintNames("TABLE1"), hasSize(2));
		assertThat(sut.findConstraintNames("TABLE2"), hasSize(1));
		assertThat(sut.findConstraintNames("TABLE3"), hasSize(1));
		assertThat(sut.findConstraintNames("TABLE4"), hasSize(1));
		assertThat(sut.findConstraintNames("TABLE5"), hasSize(1));
		assertThat(sut.findConstraintNames("TABLE6"), hasSize(0));
	}

}
