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


import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.db.TableField;
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
		try (TestDataFixture testData = new StandardTestDataFixture(original,
				destination, transformations)) {
			testData.populateDatabases();
			testData.createAnonymizer().run();
			org.dbunit.Assertion.assertEquals(
					testData.expectedDestinationDataSet(),
					testData.actualDestinationDataSet());
		}
	}
	
	@Test
	public void testCollectRulesBySite() {
		Rule leaf = new Rule(new TableField("S.LEAF.C"), "a", "");
		leaf.setTransformation(mock(TransformationStrategy.class));
		Rule parent1 = new Rule(new TableField("S.PARENT.C"), "b", "");
		parent1.setTransformation(mock(TransformationStrategy.class));
		Rule parent2 = new Rule(new TableField("S.PARENT.C"), "c", "");
		assumeThat(parent1.getTableField(), is(parent2.getTableField()));
		parent2.setTransformation(mock(TransformationStrategy.class));
		Rule parentsParent = new Rule(new TableField("S.TOP.C"), "d", "");
		parentsParent.setTransformation(mock(TransformationStrategy.class));
		parentsParent.addDependant(parent1.getTableField());
		parent1.addDependant(leaf.getTableField());
		stubConfig.addRule(parentsParent);
		stubConfig.addRule(leaf);
		stubConfig.addRule(parent1);
		stubConfig.addRule(parent2);
		sut.collectRulesBySite();
		assertThat(sut.comprehensiveRulesBySite.keySet(), containsInAnyOrder(
				leaf.getTableField(), parent1.getTableField(),
				parentsParent.getTableField()));
		assertThat(sut.comprehensiveRulesBySite.get(parentsParent.getTableField()),
				contains(parentsParent));
		assumeThat(leaf.transitiveParents(),
				contains(parentsParent, parent1, parent2));
		assertThat(sut.comprehensiveRulesBySite.get(parent1.getTableField()),
				contains(parentsParent, parent1, parent2));
		assertThat(sut.comprehensiveRulesBySite.get(parent2.getTableField()),
				contains(parentsParent, parent1, parent2));
		assertThat(sut.comprehensiveRulesBySite.get(leaf.getTableField()),
				contains(parentsParent, parent1, parent2, leaf));
		
		parentsParent.setTransformation(mock(NoOperationStrategy.class));
		parent2.setTransformation(mock(NoOperationStrategy.class));
		// when parentsParent and parent2 are no-ops,
		// only leaf and parent1 should be left in each chain
		sut.collectRulesBySite();
		assertThat(sut.comprehensiveRulesBySite.keySet(), containsInAnyOrder(
				leaf.getTableField(), parent1.getTableField()));
		assertThat(sut.comprehensiveRulesBySite.get(parent1.getTableField()),
				contains(parent1));
		assertThat(sut.comprehensiveRulesBySite.get(parent2.getTableField()),
				contains(parent1));
		assertThat(sut.comprehensiveRulesBySite.get(leaf.getTableField()),
				contains(parent1, leaf));
	}
	
	@Test
	public void testRulesBySiteMultimapImplementation()
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		sut.collectRulesBySite();
		Method create = sut.comprehensiveRulesBySite.getClass().getMethod("create");
		@SuppressWarnings("unchecked")
		Multimap<Integer, Integer> mmap = (Multimap<Integer, Integer>) create.invoke(null);
		mmap.put(5, 1);
		mmap.put(4, 3);
		mmap.put(4, 2);
		mmap.put(4, 1);
		mmap.put(4, 3);
		assertThat(mmap.keySet(), contains(5, 4));
		assertThat(mmap.get(4), contains(3, 2, 1));
	}

}
