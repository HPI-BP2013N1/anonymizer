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


import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.PreparationFailedException;
import de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy.AdditionalInfo;
import de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy.ColumnValueParameters;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;

public class UniformDistributionStrategyTest {

	private static final int NUMBER_OF_A = 5;
	private static final int NUMBER_OF_B = 1;
	private static final int NUMBER_OF_C = 2;
	private UniformDistributionStrategy sut;
	private TestDataFixture testData;
	private Rule rule;
	private Anonymizer anonymizerMock = Mockito.mock(Anonymizer.class);
	private RowRetainService retainServiceMock;

	@Before
	public void createSubjectUnderTest() throws Exception {
		Config stubConfig = StandardTestDataFixture.makeStubConfig();
		Scope scope = new Scope();
		testData = new StandardTestDataFixture(stubConfig, scope);
		sut = new UniformDistributionStrategy(anonymizerMock,
				testData.originalDbConnection,
				testData.transformationDbConnection);
		retainServiceMock = Mockito.mock(RowRetainService.class);
		when(anonymizerMock.getRetainService()).thenReturn(retainServiceMock);
		
		rule = new Rule(new TableField("aTable.aColumn"), "", "");
	}

	@Test
	public void testSetUpTransformation() throws PreparationFailedException {
	}
	
	@Test
	public void testTransformIntegers() throws SQLException, TransformationKeyNotFoundException, PreparationFailedException {
		try (Statement ddlStatement = testData.originalDbConnection.createStatement()) {
			ddlStatement.executeUpdate("CREATE TABLE IntegerTable ("
					+ "aColumn INT)");
		}
		testData.originalDbConnection.setAutoCommit(false);
		try (PreparedStatement insertStatement = testData.originalDbConnection.prepareStatement(
				"INSERT INTO IntegerTable (aColumn) VALUES (?)")) {
			insertStatement.setInt(1, 1);
			for (int i = 0; i < NUMBER_OF_A; i++)
				insertStatement.addBatch();
			insertStatement.setInt(1, 2);
			for (int i = 0; i < NUMBER_OF_B; i++)
				insertStatement.addBatch();
			insertStatement.setInt(1, 3);
			for (int i = 0; i < NUMBER_OF_C; i++)
				insertStatement.addBatch();
			insertStatement.executeBatch();
			testData.originalDbConnection.commit();
		} finally {
			testData.originalDbConnection.setAutoCommit(true);
		}
		rule = new Rule(new TableField("IntegerTable.aColumn"), "", "");
		sut.setUpTransformation(Lists.newArrayList(rule));
		
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform(2, rule, null)),
				hasItems((Object) 2));
		assertDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, 1);
		assertDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, 3);
	}

	@Test
	public void testTransform() throws SQLException, TransformationKeyNotFoundException, PreparationFailedException {
		try (Statement ddlStatement = testData.originalDbConnection.createStatement()) {
			ddlStatement.executeUpdate("CREATE TABLE aTable ("
					+ "aColumn VARCHAR(20))");
		}
		testData.originalDbConnection.setAutoCommit(false);
		try (PreparedStatement insertStatement = testData.originalDbConnection.prepareStatement(
				"INSERT INTO aTable (aColumn) VALUES (?)")) {
			insertStatement.setString(1, "A");
			for (int i = 0; i < NUMBER_OF_A; i++)
				insertStatement.addBatch();
			insertStatement.setString(1, "B");
			for (int i = 0; i < NUMBER_OF_B; i++)
				insertStatement.addBatch();
			insertStatement.setString(1, "C");
			for (int i = 0; i < NUMBER_OF_C; i++)
				insertStatement.addBatch();
			insertStatement.executeBatch();
			testData.originalDbConnection.commit();
		} finally {
			testData.originalDbConnection.setAutoCommit(true);
		}
		sut.setUpTransformation(Lists.newArrayList(rule));
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform("B", rule, null)),
				contains((Object) "B"));
		assertDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, "A");
		assertDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, "C");
	}

	private void assertDeletedAndRetained(int previousNumber,
			int targetNumber, Object oldValue) throws SQLException {
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		when(rowReaderMock.getCurrentTable()).thenReturn("aTable");
		when(rowReaderMock.getObject("aColumn")).thenReturn(oldValue);
		for (int i = 0; i < previousNumber - targetNumber; i++)
			assertThat("First occurences of larger categories should be deleted",
					sut.transform(oldValue, rule, rowReaderMock),
					emptyIterable());
		for (int i = previousNumber - targetNumber; i < previousNumber; i++)
			assertThat("Later occurences of larger categories should be retained",
					sut.transform(oldValue, rule, rowReaderMock),
					contains(equalTo(oldValue)));
	}
	
	private void assertNumberDeletedAndRetained(int previousNumber,
			int targetNumber, String oldPrefix) throws SQLException {
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		when(rowReaderMock.getCurrentTable()).thenReturn("aTable");
		for (int i = 0; i < previousNumber - targetNumber; i++) {
			when(rowReaderMock.getObject("aColumn")).thenReturn(oldPrefix+i);
			assertThat("First occurences of larger categories should be deleted",
					sut.transform(oldPrefix+i, rule, rowReaderMock),
					emptyIterable());
		}
		for (int i = previousNumber - targetNumber; i < previousNumber; i++) {
			when(rowReaderMock.getObject("aColumn")).thenReturn(oldPrefix+i);
			assertThat("Later occurences of larger categories should be retained",
					sut.transform(oldPrefix+i, rule, rowReaderMock),
					contains(equalTo((Object) (oldPrefix+i))));
		}
	}

	@Test
	public void testSubstringTransform() throws SQLException, PreparationFailedException, TransformationKeyNotFoundException {
		try (Statement ddlStatement = testData.originalDbConnection.createStatement()) {
			ddlStatement.executeUpdate("CREATE TABLE aTable ("
					+ "aColumn VARCHAR(20))");
		}
		testData.originalDbConnection.setAutoCommit(false);
		try (PreparedStatement insertStatement = testData.originalDbConnection.prepareStatement(
				"INSERT INTO aTable (aColumn) VALUES (?)")) {
			for (int i = 0; i < NUMBER_OF_A; i++) {
				insertStatement.setString(1, "A"+i);
				insertStatement.addBatch();
			}
			for (int i = 0; i < NUMBER_OF_B; i++) {
				insertStatement.setString(1, "B"+i);
				insertStatement.addBatch();
			}
			for (int i = 0; i < NUMBER_OF_C; i++) {
				insertStatement.setString(1, "C"+i);
				insertStatement.addBatch();
			}
			insertStatement.executeBatch();
			testData.originalDbConnection.commit();
		} finally {
			testData.originalDbConnection.setAutoCommit(true);
		}
		
		rule = new Rule(new TableField("aTable.aColumn"), "", "SUBSTR(..., 1, 1)");
		sut.setUpTransformation(Lists.newArrayList(rule));
		
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform("B0", rule, null)),
				contains((Object) "B0"));
		assertNumberDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, "A");
		assertNumberDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, "C");
	}
	
	@Test
	public void testRetainedRowsHandling() throws SQLException, PreparationFailedException, TransformationKeyNotFoundException {
		try (Statement ddlStatement = testData.originalDbConnection.createStatement()) {
			ddlStatement.executeUpdate("CREATE TABLE aTable ("
					+ "aColumn VARCHAR(20), otherColumn INT, "
					+ "PRIMARY KEY (aColumn, otherColumn))");
		}
		testData.originalDbConnection.setAutoCommit(false);
		try (PreparedStatement insertStatement = testData.originalDbConnection.prepareStatement(
				"INSERT INTO aTable (aColumn, otherColumn) VALUES (?, ?)")) {
			insertStatement.setString(1, "A");
			for (int i = 0; i < NUMBER_OF_A; i++) {
				insertStatement.setInt(2, i);
				insertStatement.addBatch();
			}
			insertStatement.setString(1, "B");
			for (int i = 0; i < NUMBER_OF_B; i++) {
				insertStatement.setInt(2, i);
				insertStatement.addBatch();
			}
			insertStatement.setString(1, "C");
			for (int i = 0; i < NUMBER_OF_C; i++) {
				insertStatement.setInt(2, i);
				insertStatement.addBatch();
			}
			insertStatement.executeBatch();
			testData.originalDbConnection.commit();
		} finally {
			testData.originalDbConnection.setAutoCommit(true);
		}
		int retainedColumnValue = 0;
		when(retainServiceMock.currentRowShouldBeRetained(anyString(),
				eq("aTable"),
				argThat(new ResultSetRowReaderMatcher(
						"otherColumn", retainedColumnValue))))
				.thenReturn(true);
		sut.setUpTransformation(Lists.newArrayList(rule));
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform("B", rule, null)),
				contains((Object) "B"));
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		when(rowReaderMock.getObject("aColumn")).thenReturn("A");
		when(rowReaderMock.getObject("otherColumn")).thenReturn(retainedColumnValue);
		when(rowReaderMock.getCurrentTable()).thenReturn("aTable");
		assertThat("Retained rows must not be deleted",
				sut.transform("A", rule, rowReaderMock),
				contains(equalTo((Object) "A")));
		int numberOfKeptRows = 0;
		for (int i = 0; i < NUMBER_OF_A; i++) {
			when(rowReaderMock.getObject("otherColumn")).thenReturn(i);
			if (sut.transform("A", rule, rowReaderMock).iterator().hasNext()) {
				numberOfKeptRows++;
			}
		}
		assertThat("if a row should be retained another one should be deleted instead",
				numberOfKeptRows, is(NUMBER_OF_B));
		numberOfKeptRows = 0;
		for (int i = 0; i < NUMBER_OF_C; i++) {
			when(rowReaderMock.getObject("otherColumn")).thenReturn(i);
			if (sut.transform("C", rule, rowReaderMock).iterator().hasNext()) {
				numberOfKeptRows++;
			}
		}
		assertThat("if a row should be retained another one should be deleted instead",
				numberOfKeptRows, is(NUMBER_OF_B));
	}
	
	static class ResultSetRowReaderMatcher extends ArgumentMatcher<ResultSetRowReader> {
		private String column;
		private Object matchValue;
		
		public ResultSetRowReaderMatcher(String column, Object matchValue) {
			this.column = column;
			this.matchValue = matchValue;
		}
		
		@Override
		public boolean matches(Object argument) {
			ResultSetRowReader rowReader = (ResultSetRowReader) argument;
			try {
				return rowReader.getObject(column).equals(matchValue);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}
	}

	@Test
	public void testAdditionalInfoParsing() {
		AdditionalInfo info;
		info = AdditionalInfo.parse("SUBSTR(..., 1, 2)", "A");
		assertThat(info.columnExpressionWithPlaceholder, is("SUBSTR(..., 1, 2)"));
		assertThat(info.lowerRowThreshold, is(0f));
		info = AdditionalInfo.parse("1 + ...; REQUIRE MIN 10%", "A");
		assertThat(info.columnExpressionWithPlaceholder, is("1 + ..."));
		assertThat(info.lowerRowThreshold, is(0.1f));
		info = AdditionalInfo.parse("1 + ...; REQUIRE at least 10%", "B");
		assertThat(info.columnExpressionWithPlaceholder, is("1 + ..."));
		assertThat(info.lowerRowThreshold, is(0.1f));
		info = AdditionalInfo.parse("1 + ...; REQUIRE MIN 10", "A");
		assertThat(info.columnExpressionWithPlaceholder, is("1 + ..."));
		assertThat(info.lowerRowThreshold, is(10f));
		info = AdditionalInfo.parse("1 + ...; REQUIRE at MIN 76", "A");
		assertThat(info.columnExpressionWithPlaceholder, is("1 + ..."));
		assertThat(info.lowerRowThreshold, is(76f));
		info = AdditionalInfo.parse("1 + ...; REQUIRE at MINimum 82.1 %", "A");
		assertThat(info.columnExpressionWithPlaceholder, is("1 + ..."));
		assertThat(info.lowerRowThreshold, is(0.821f));
		info = AdditionalInfo.parse("REQUIRE at MINimum 82.1%; ... || 'B'", "A");
		assertThat(info.columnExpressionWithPlaceholder, is(" ... || 'B'"));
		assertThat(info.lowerRowThreshold, is(0.821f));
	}
	
	@Test
	public void testShouldBeRemoved() throws SQLException {
		ResultSetRowReader row = mock(ResultSetRowReader.class);
		ColumnValueParameters valueParameters = sut.new ColumnValueParameters();
		valueParameters.columnExpression = "A";
		valueParameters.existingCardinalities = new TreeMap<>();
		valueParameters.targetCardinality = 2l;
		// should be removed if targetCardinality is exceeded
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 1));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 2));
		assertTrue(sut.shouldBeRemoved(row, valueParameters, 3));
		valueParameters.targetCardinality = 1l;
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 1));
		assertTrue(sut.shouldBeRemoved(row, valueParameters, 2));
		assertTrue(sut.shouldBeRemoved(row, valueParameters, 3));
		// should be removed if lowerThreshold is not met
		valueParameters.targetCardinality = 3l;
		valueParameters.lowerThreshold = 2l;
		assertTrue(sut.shouldBeRemoved(row, valueParameters, 1));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 2));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 3));
		assertTrue(sut.shouldBeRemoved(row, valueParameters, 4));
		// always retain if marked so
		when(retainServiceMock.currentRowShouldBeRetained(anyString(), anyString(), eq(row)))
		.thenReturn(true);
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 1));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 2));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 3));
		assertFalse(sut.shouldBeRemoved(row, valueParameters, 4));
	}

}
