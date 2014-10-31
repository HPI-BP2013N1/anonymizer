package de.hpi.bp2013n1.anonymizer;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI-BP2013N1
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


import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;

public class AnonymizerTest {

	Config stubConfig;
	Scope stubScope;
	Connection original, transformations, destination;
	private Anonymizer sut;

	@Before
	public void createStubsAndAnonymizer() {
		stubConfig = new Config();
		stubScope = new Scope();
		try {
			original = DriverManager.getConnection("jdbc:h2:mem:");
			transformations = DriverManager.getConnection("jdbc:h2:mem:");
			destination = DriverManager.getConnection("jdbc:h2:mem:");
		} catch (SQLException e) {
			fail("Could not create DB stubs");
		}
		sut = new Anonymizer(stubConfig, stubScope);
		sut.useDatabases(original, destination, transformations);
	}

	@Test(expected = ClassCastException.class)
	public void detectNonTransformationStrategies() throws SQLException,
			ClassNotFoundException, NoSuchMethodException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		sut.loadAndInstanciateStrategy("java.lang.Integer");
	}
	
	static class TransformationStrategyStub extends TransformationStrategy {

		public TransformationStrategyStub(
				Anonymizer anonymizer,
				Connection originalDatabase,
				Connection transformationDatabase) throws SQLException {
			super(anonymizer, originalDatabase, transformationDatabase);
		}

		@Override
		public void setUpTransformation(Collection<Rule> rule) {
		}

		@Override
		public Iterable<String> transform(Object oldValue, Rule rule, ResultSetRowReader row) {
			return Arrays.asList(new String[] { null });
		}

		@Override
		public void prepareTableTransformation(
				TableRuleMap tableRules) {
		}

		@Override
		public boolean isRuleValid(Rule rule, int type, int length,
				boolean nullAllowed) {
			return true;
		}
		
	}

	@Test
	public void allowTransformationStrategies() throws SQLException,
			ClassNotFoundException, NoSuchMethodException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		sut.loadAndInstanciateStrategy(TransformationStrategyStub.class.getName());
	}
	
	@Test
	public void transformationsAsExpected() throws Exception {
		try (TestDataFixture testData = new TestDataFixture(original,
				destination, transformations)) {
			testData.populateDatabases();
			testData.createAnonymizer().run();
			org.dbunit.Assertion.assertEquals(
					testData.expectedDestinationDataSet(), 
					testData.actualDestinationDataSet());
		}
	}

}
