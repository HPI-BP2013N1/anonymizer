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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.TransformationFailedException;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;

public class DeleteRowStrategyTest {

	private DeleteRowStrategy sut;
	private TestDataFixture testData;

	@Before
	public void setUpRetainStrategyAndRetainService()
			throws ClassNotFoundException, IOException, SQLException {
		testData = new TestDataFixture(
				TestDataFixture.makeStubConfig(), null);
		sut = new DeleteRowStrategy(null, testData.originalDbConnection,
				testData.transformationDbConnection);
	}

	@Test
	public void testTransform() throws ClassNotFoundException, IOException, SQLException, TransformationFailedException, TransformationKeyNotFoundException {
		try (Statement createTable = testData.originalDbConnection.createStatement()) {
			createTable.executeUpdate("CREATE TABLE ATABLE (ACOLUMN INT PRIMARY KEY)");
		}
		try (PreparedStatement insert = testData.originalDbConnection.prepareStatement(
				"INSERT INTO ATABLE (ACOLUMN) VALUES (?)")) {
			insert.setInt(1, 0);
			insert.addBatch();
			insert.setInt(1, 1);
			insert.addBatch();
			insert.executeBatch();
		}
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		when(rowReaderMock.getObject("ACOLUMN")).thenReturn(0);
		when(rowReaderMock.getCurrentSchema()).thenReturn("PUBLIC");
		when(rowReaderMock.getCurrentTable()).thenReturn("ATABLE");
		Rule rule = new Rule();
		rule.additionalInfo = "ACOLUMN = 0";
		assertThat("Matched rows should be deleted",
				sut.transform(0, rule, rowReaderMock), emptyIterable());
		when(rowReaderMock.getObject("ACOLUMN")).thenReturn(1);
		assertThat("Unmatched rows should not be deleted",
				sut.transform(1, rule, rowReaderMock), 
				contains(equalTo((Object) 1)));
	}

	@Test
	public void testIsRuleValid() throws RuleValidationException, SQLException {
		try (Statement createTable = testData.originalDbConnection.createStatement()) {
			createTable.executeUpdate("CREATE TABLE ATABLE (ACOLUMN INT PRIMARY KEY)");
		}
		Rule rule = new Rule();
		rule.tableField = new TableField("ATABLE", "PUBLIC");
		rule.additionalInfo = "ACOLUMN = 0";
		assertThat(sut.isRuleValid(rule, 0, 0, false), is(true));
		rule.additionalInfo = "FOO = 'BAR'";
		assertThat(sut.isRuleValid(rule, 0, 0, false), is(false));
		rule.additionalInfo = "= ? '123'";
		assertThat(sut.isRuleValid(rule, 0, 0, false), is(false));
		rule.additionalInfo = "ACOLUMN = 0";
		rule.tableField.table = "XYZ";
		assertThat(sut.isRuleValid(rule, 0, 0, false), is(false));		
	}
}
