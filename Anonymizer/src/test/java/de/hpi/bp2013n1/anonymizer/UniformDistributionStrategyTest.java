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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.PreparationFailedExection;
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

	@Before
	public void createSubjectUnderTest() throws Exception {
		Config stubConfig = TestDataFixture.makeStubConfig();
		Scope scope = new Scope();
		testData = new TestDataFixture(stubConfig, scope);
		sut = new UniformDistributionStrategy(null,
				testData.originalDbConnection,
				testData.transformationDbConnection);
		
		rule = new Rule();
		rule.tableField = new TableField("aTable.aColumn");
	}

	@Test
	public void testSetUpTransformation() throws PreparationFailedExection {
		// TODO: implement
	}
	
	@Test
	public void testTransformIntegers() throws SQLException, TransformationKeyNotFoundException, PreparationFailedExection {
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
		rule = new Rule();
		rule.tableField = new TableField("IntegerTable.aColumn");
		sut.setUpTransformation(Lists.newArrayList(rule));
		
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform(2, rule, null)),
				hasItems((Object) 2));
		assertDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, 1);
		assertDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, 3);
	}

	@Test
	public void testTransform() throws SQLException, TransformationKeyNotFoundException, PreparationFailedExection {
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
			int targetNumber, Object oldValue) throws SQLException,
			TransformationKeyNotFoundException {
		for (int i = 0; i < previousNumber - targetNumber; i++)
			assertThat("First occurences of larger categories should be deleted",
					sut.transform(oldValue, rule, null), 
					emptyIterable());
		for (int i = previousNumber - targetNumber; i < previousNumber; i++)
			assertThat("Later occurences of larger categories should be retained",
					sut.transform(oldValue, rule, null),
					contains(equalTo(oldValue)));
	}
	
	private void assertNumberDeletedAndRetained(int previousNumber,
			int targetNumber, String oldPrefix) throws SQLException,
			TransformationKeyNotFoundException {
		for (int i = 0; i < previousNumber - targetNumber; i++)
			assertThat("First occurences of larger categories should be deleted",
					sut.transform(oldPrefix+i, rule, null), 
					emptyIterable());
		for (int i = previousNumber - targetNumber; i < previousNumber; i++)
			assertThat("Later occurences of larger categories should be retained",
					sut.transform(oldPrefix+i, rule, null),
					contains(equalTo((Object) (oldPrefix+i))));
	}

	@Test
	public void testSubstringTransform() throws SQLException, PreparationFailedExection, TransformationKeyNotFoundException {
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
		rule.additionalInfo = "SUBSTR(..., 1, 1)";
		sut.setUpTransformation(Lists.newArrayList(rule));
		
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform("B0", rule, null)),
				contains((Object) "B0"));
		assertNumberDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, "A");
		assertNumberDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, "C");
	}

	
	@Test
	public void testPrepareTableTransformation() {
		// TODO: implement
	}

	@Test
	public void testIsRuleValid() {
		// TODO: implement
	}

}
