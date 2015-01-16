package de.hpi.bp2013n1.anonymizer.analyzer;

/*
 * #%L
 * Analyzer
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


import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.appliesStrategyTo;
import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.usesStrategy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.hpi.bp2013n1.anonymizer.StandardTestDataFixture;
import de.hpi.bp2013n1.anonymizer.TestDataFixture;
import de.hpi.bp2013n1.anonymizer.analyzer.Analyzer.FatalError;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class AnalyzerTest {

	static Config config;
	static File logFile;
	private Analyzer sut;
	static TestDataFixture testData;
	
	@Before
	public void setSchema() throws SQLException {
		testData.setSchema();
	}

	@BeforeClass
	public static void setUpDatabases() throws Exception {
		testData = new StandardTestDataFixture();
		testData.populateDatabases();
	}
	
	@Before
	public void createAnalyzerAndImportData() throws Exception {
		readConfig();
		sut = new Analyzer(testData.getOriginalDbConnection(), config,
				testData.getScope());
		testData.populateTables(config.schemaName);
	}
	
	private static void readConfig() throws Exception {
		config = new Config();
		config.readFromURL(AnalyzerTest.class.getResource(
				"AnalyzerTestConfig.txt"));
	}

	@Test
	public void outputFileIsCreated() throws IOException, FatalError {
		File outputFile = runAnalyzer();
		assertTrue(outputFile.exists());
	}

	private File runAnalyzer() throws IOException, FatalError {
		File outputFile = File.createTempFile("analyzerOutput", null);
		outputFile.delete();
		sut.run(outputFile.getPath());
		outputFile.deleteOnExit();
		return outputFile;
	}
	
	@Test
	public void deleteWrongCharwiseRule() throws Exception {
		File outputFile = runAnalyzer();
		Config newConfig = new Config();
		newConfig.readFromFile(outputFile.getPath());
		assertThat(newConfig.rules, not(hasItem(
				usesStrategy(is("S"), is("XYZKPPPP")))));
		assertThat(newConfig.rules, hasItem(
				appliesStrategyTo(
						equalTo(new TableField("VISITOR.SURNAME", config.schemaName)),
						is("P"),
						isEmptyString())));
		assertThat(newConfig.rules, hasItem(
				appliesStrategyTo(
						equalTo(new TableField("VISITOR.ADDRESS", config.schemaName)),
						is("D"),
						isEmptyString())));
	}
	
	@Test
	public void existingDependantsValidation() throws SQLException {
		Rule validatedRule = new Rule(
				new TableField("VISITOR.SURNAME", config.schemaName), "P", "");
		validatedRule.addDependant(
				new TableField("doesnot.exist", config.schemaName));
		validatedRule.addDependant(
				new TableField("doesnot.existeither", config.schemaName));
		TableField firstValidDependant =
				new TableField("VISIT.VISITORSURNAME", config.schemaName);
		validatedRule.addDependant(firstValidDependant);
		validatedRule.addDependant(
				new TableField("another.fool", config.schemaName));
		TableField secondValidDependant =
				new TableField("VISITOR.NAME", config.schemaName);
		// this is not correct regarding the test schema but it is a valid config
		validatedRule.addDependant(secondValidDependant);
		sut.validateExistingDependants(validatedRule,
				testData.getOriginalDbConnection().getMetaData());
		assertThat(validatedRule.getDependants(),
				contains(firstValidDependant, secondValidDependant));
	}
	
	TableField tableField(String tableColumn) {
		return new TableField(tableColumn, config.schemaName);
	}
	
	@Test
	public void foreignKeyDependantsTest() throws SQLException {
		DatabaseMetaData metaData = testData.getOriginalDbConnection().getMetaData();
		TableField originField = tableField("VISITOR.NAME");
		Rule plainRule = new Rule(originField, "P", "");
		config.rules.add(plainRule);
		sut.initializeRulesByTableField();
		sut.findDependantsByForeignKeys(metaData);
		TableField dependentField = tableField("VISIT.VISITORNAME");
		assertThat(plainRule.getDependants(), contains(dependentField));
		config.rules.remove(plainRule);
		Rule fullRule = new Rule(originField, "P", "",
				Sets.newHashSet(dependentField));
		config.rules.add(fullRule);
		sut.initializeRulesByTableField();
		sut.findDependantsByForeignKeys(metaData);
		assertThat(fullRule.getDependants(), contains(dependentField));
		// TODO: test higher-order (transitive) foreign key dependencies
		// i.e. A -> B -> C implies A -> C
	}
	
	@Test
	public void possibleDependantsTest() throws SQLException {
		DatabaseMetaData metaData = testData.getOriginalDbConnection().getMetaData();
		TableField originField = tableField("VISITOR.NAME");
		TableField dependentField = tableField("VISIT.VISITORNAME");
		List<TableField> matchingFields = Lists.newArrayList(
				tableField("VISITOR.SURNAME"),
				dependentField,
				tableField("VISIT.VISITORSURNAME"),
				tableField("PRODUCTBUYER.BUYERNAME"),
				tableField("BUYERDETAILS.BUYERNAME"),
				tableField("CINEMACASHLOG.VISITORNAME"),
				tableField("CINEMACASHLOG.VISITORSURNAME"));
		List<TableField> matchingButIndependentFields = Lists.newArrayList(matchingFields);
		matchingButIndependentFields.remove(dependentField);
		sut.initializeRulesByTableField();
		
		Rule plainRule = new Rule(originField, "P", "");
		sut.findPossibleDependantsByName(plainRule, metaData);
		assertThat(plainRule.getPotentialDependants(),
				containsInAnyOrder(matchingFields.toArray()));
		
		Rule fullRule = new Rule(originField, "P", "",
				Sets.newHashSet(dependentField));
		sut.config.addRule(fullRule);
		sut.findPossibleDependantsByName(fullRule, metaData);
		assertThat(fullRule.getPotentialDependants(),
				containsInAnyOrder(matchingButIndependentFields.toArray()));
		sut.config.removeRule(fullRule);
		
		Rule plainRuleWithPossibleDependants = new Rule(originField, "P", "");
		plainRuleWithPossibleDependants.getPotentialDependants().addAll(matchingButIndependentFields);
		sut.config.addRule(plainRuleWithPossibleDependants);
		sut.findPossibleDependantsByName(plainRuleWithPossibleDependants, metaData);
		assertThat(plainRuleWithPossibleDependants.getPotentialDependants(),
				containsInAnyOrder(matchingFields.toArray()));
		sut.config.removeRule(plainRuleWithPossibleDependants);
		
		Rule fullRuleWithPossibleDependants = new Rule(originField, "P", "",
				Sets.newHashSet(dependentField));
		fullRuleWithPossibleDependants.getPotentialDependants().addAll(matchingButIndependentFields);
		sut.config.addRule(fullRuleWithPossibleDependants);
		sut.findPossibleDependantsByName(fullRuleWithPossibleDependants, metaData);
		assertThat(fullRuleWithPossibleDependants.getPotentialDependants(),
				containsInAnyOrder(matchingButIndependentFields.toArray()));
		sut.config.removeRule(fullRuleWithPossibleDependants);
	}
	
	@Test
	public void noUnneededNoOpRules() throws IOException, FatalError, DependantWithoutRuleException, MalformedException {
		File outputFile = runAnalyzer();
		Config newConfig = new Config();
		newConfig.readFromFile(outputFile.getPath());
		for (Rule rule : newConfig.rules) {
			if (!rule.getStrategy().equals(Config.NO_OP_STRATEGY_KEY))
				continue;
			if (rule.getDependants().isEmpty())
				assertThat("There must be no no-op rule without possible or real dependants",
						rule.getPotentialDependants(), is(not(empty())));
		}
	}
}
