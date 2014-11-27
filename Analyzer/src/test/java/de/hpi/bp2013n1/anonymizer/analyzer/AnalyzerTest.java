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


import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.h2.tools.RunScript;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.analyzer.Analyzer.FatalError;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class AnalyzerTest {

	// TODO: alleviate code duplication with Anonymizer tests
	static Config config;
	static Scope scope;
	static Connection originalDbConnection;
	static Connection transformationDbConnection;
	static Connection destinationDbConnection;
	static File logFile;
	private Analyzer sut;
	
	@Before
	public void setSchema() throws SQLException {
		staticSetSchema();
	}

	private static void staticSetSchema() throws SQLException {
		try (Statement s = originalDbConnection.createStatement()) {
			s.execute("SET SCHEMA = " + config.schemaName);
		}
		// originalDbConnection.setSchema(config.schemaName);
		try (Statement s = transformationDbConnection.createStatement()) {
			s.execute("SET SCHEMA = " + config.schemaName);
		}
		// transformationDbConnection.setSchema(config.schemaName);
		try (Statement s = destinationDbConnection.createStatement()) {
			s.execute("SET SCHEMA = " + config.schemaName);
		}
		// destinationDbConnection.setSchema(config.schemaName);
	}

	@BeforeClass
	public static void setUpDatabases() throws Exception {
		createDbConnections();
		populateDatabases();
	}
	
	@Before
	public void createAnalyzer() throws Exception {
		readConfig();
		sut = new Analyzer(originalDbConnection, config, scope);
	}
	
	private static void createDbConnections() throws Exception {
		readConfig();
		scope = new Scope();
		scope.readFromURL(AnalyzerTest.class.getResource("/de/hpi/bp2013n1/anonymizer/testscope.txt"));
		originalDbConnection = DatabaseConnector.connect(config.originalDB);
		transformationDbConnection = DatabaseConnector.connect(config.transformationDB);
		destinationDbConnection = DatabaseConnector.connect(config.destinationDB);
	}

	private static void readConfig() throws Exception {
		config = new Config();
		config.readFromURL(AnalyzerTest.class.getResource(
				"AnalyzerTestConfig.txt"));
	}

	private static void populateDatabases() throws DatabaseUnitException,
			IOException, SQLException {
		executeDdlScript("/de/hpi/bp2013n1/anonymizer/testschema.ddl.sql", originalDbConnection);
		executeDdlScript("/de/hpi/bp2013n1/anonymizer/testschema.ddl.sql", destinationDbConnection);
		executeDdlScript("/de/hpi/bp2013n1/anonymizer/testpseudonymsschema.ddl.sql", transformationDbConnection);
	}

	@Before
	public void importData() throws DatabaseUnitException, IOException,
			SQLException {
		importData(originalDbConnection, config.schemaName, "/de/hpi/bp2013n1/anonymizer/testdata.xml");
		importData(transformationDbConnection, config.schemaName,
				"/de/hpi/bp2013n1/anonymizer/testpseudonyms.xml");
	}

	private static void executeDdlScript(String scriptFileName,
			Connection dbConnection) throws SQLException, IOException {
		try (InputStream ddlStream = AnalyzerTest.class
				.getResourceAsStream(scriptFileName);
				InputStreamReader ddlReader = new InputStreamReader(ddlStream)) {
			RunScript.execute(dbConnection, ddlReader);
		}
	}

	private static void importData(Connection connection, String schemaName,
			String dataSetFilename) throws DatabaseUnitException, IOException,
			SQLException {
		IDatabaseConnection db = new DatabaseConnection(connection, schemaName);
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		IDataSet data = dataSetBuilder.build(AnalyzerTest.class
				.getResourceAsStream(dataSetFilename));
		DatabaseOperation.CLEAN_INSERT.execute(db, data);
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
		validatedRule.dependants.add(
				new TableField("doesnot.exist", config.schemaName));
		validatedRule.dependants.add(
				new TableField("doesnot.existeither", config.schemaName));
		TableField firstValidDependant = 
				new TableField("VISIT.VISITORSURNAME", config.schemaName);
		validatedRule.dependants.add(firstValidDependant);
		validatedRule.dependants.add(
				new TableField("another.fool", config.schemaName));
		TableField secondValidDependant = 
				new TableField("VISITOR.NAME", config.schemaName);
		validatedRule.dependants.add(secondValidDependant); // not useful but valid
		sut.validateExistingDependants(validatedRule, 
				originalDbConnection.getMetaData());
		assertThat(validatedRule.dependants, 
				contains(firstValidDependant, secondValidDependant));
	}
	
	TableField tableField(String tableColumn) {
		return new TableField(tableColumn, config.schemaName);
	}
	
	@Test
	public void foreignKeyDependantsTest() throws SQLException {
		DatabaseMetaData metaData = originalDbConnection.getMetaData();
		TableField originField = tableField("VISITOR.NAME");
		Rule plainRule = new Rule(originField, "P", "");
		config.rules.add(plainRule);
		sut.initializeRulesByTableField();
		sut.findDependantsByForeignKeys(metaData);
		TableField dependentField = tableField("VISIT.VISITORNAME");
		assertThat(plainRule.dependants, contains(dependentField));
		config.rules.remove(plainRule);
		Rule fullRule = new Rule(originField, "P", "", 
				Lists.newArrayList(dependentField));
		config.rules.add(fullRule);
		sut.initializeRulesByTableField();
		sut.findDependantsByForeignKeys(metaData);
		assertThat(fullRule.dependants, contains(dependentField));
		// TODO: test higher-order (transitive) foreign key dependencies 
		// i.e. A -> B -> C implies A -> C
	}
	
	@Test
	public void possibleDependantsTest() throws SQLException {
		DatabaseMetaData metaData = originalDbConnection.getMetaData();
		TableField originField = tableField("VISITOR.NAME");
		TableField dependentField = tableField("VISIT.VISITORNAME");
		List<TableField> matchingFields = Lists.newArrayList(
				tableField("VISITOR.SURNAME"),
				dependentField,
				tableField("VISIT.VISITORSURNAME"),
				tableField("PRODUCTBUYER.BUYERNAME"),
				tableField("BUYERDETAILS.BUYERNAME"));
		List<TableField> matchingButIndependentFields = Lists.newArrayList(matchingFields);
		matchingButIndependentFields.remove(dependentField);
		sut.initializeRulesByTableField();
		
		Rule plainRule = new Rule(originField, "P", "");
		sut.findPossibleDependantsByName(plainRule, metaData);
		assertThat(plainRule.potentialDependants,
				containsInAnyOrder(matchingFields.toArray()));
		
		Rule fullRule = new Rule(originField, "P", "", 
				Lists.newArrayList(dependentField));
		sut.findPossibleDependantsByName(fullRule, metaData);
		assertThat(fullRule.potentialDependants,
				containsInAnyOrder(matchingButIndependentFields.toArray()));
		
		Rule plainRuleWithPossibleDependants = new Rule(originField, "P", "");
		plainRuleWithPossibleDependants.potentialDependants.addAll(matchingButIndependentFields);
		sut.findPossibleDependantsByName(plainRuleWithPossibleDependants, metaData);
		assertThat(plainRuleWithPossibleDependants.potentialDependants,
				containsInAnyOrder(matchingFields.toArray()));
		
		Rule fullRuleWithPossibleDependants = new Rule(originField, "P", "",
				Lists.newArrayList(dependentField));
		fullRuleWithPossibleDependants.potentialDependants.addAll(matchingButIndependentFields);
		sut.findPossibleDependantsByName(fullRuleWithPossibleDependants, metaData);
		assertThat(fullRuleWithPossibleDependants.potentialDependants,
				containsInAnyOrder(matchingButIndependentFields.toArray()));
	}
	
	@Test
	public void noUnneededNoOpRules() throws IOException, FatalError, DependantWithoutRuleException {
		File outputFile = runAnalyzer();
		Config newConfig = new Config();
		newConfig.readFromFile(outputFile.getPath());
		for (Rule rule : newConfig.rules) {
			if (!rule.strategy.equals(Analyzer.NO_OP_STRATEGY_KEY))
				continue;
			if (rule.dependants.isEmpty())
				assertThat("There must be no no-op rule without possible or real dependants",
						rule.potentialDependants, is(not(empty())));
		}
	}
}
