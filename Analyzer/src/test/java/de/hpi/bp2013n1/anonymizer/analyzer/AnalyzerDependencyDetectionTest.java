package de.hpi.bp2013n1.anonymizer.analyzer;

import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.isLikeRule;
import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.RuleMatcher.rule;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;

import org.dbunit.DatabaseUnitException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.TestSpecificTestDataFixture;
import de.hpi.bp2013n1.anonymizer.analyzer.Analyzer.FatalError;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class AnalyzerDependencyDetectionTest {
	
	TestSpecificTestDataFixture testData;
	private Analyzer sut;
	
	@Before
	public void setUp() throws ClassNotFoundException, IOException,
			DependantWithoutRuleException, SQLException, DatabaseUnitException,
			FatalError {
		testData = new TestSpecificTestDataFixture(this);
		testData.populateDatabases();
		sut = new Analyzer(testData.getOriginalDbConnection(),
				testData.getConfig(), testData.getScope());
		sut.setUpTransformationStrategies();
		sut.validateRulesAndAddDependants();
	}
	
	@Test
	public void testSimpleDependencies() throws FatalError, SQLException {
		Config resultConfig = sut.config;
		String strategyKey = resultConfig.strategyMapping.entrySet().iterator().next().getKey();
		assertThat(resultConfig.rules, hasItem(
				isLikeRule(
						is(new TableField("TABLE1.E", resultConfig.schemaName)),
						is(strategyKey),
						empty(),
						containsInAnyOrder(new TableField("TABLE2.E1",
								resultConfig.schemaName)),
						isEmptyOrNullString())));
		TableField expectedNewRuleTableField = new TableField("TABLE1.F",
				resultConfig.schemaName);
		assertThat("A rule for the second PK column should be added",
				resultConfig.rules, hasItem(
				Matchers.<Rule>hasProperty("tableField", equalTo(expectedNewRuleTableField))));
		assertThat("No rule for only-dependend columns should be added",
				resultConfig.rules, not(
						either(hasItem(rule().forAttribute("TABLE2.E1")))
						.or(hasItem(rule().forAttribute("TABLE2.F1")))));
		assertThat(resultConfig.rules, hasItem(
				isLikeRule(
						equalTo(new TableField("TABLE1.E",
								resultConfig.schemaName)),
						is(strategyKey),
						empty(),
						containsInAnyOrder(new TableField("TABLE2.E1",
								resultConfig.schemaName)),
						isEmptyOrNullString())));
		assertThat(resultConfig.rules, hasItem(
				isLikeRule(
						equalTo(expectedNewRuleTableField),
						isEmptyOrNullString(),
						empty(),
						containsInAnyOrder(new TableField("TABLE2.F1",
								resultConfig.schemaName)),
						isEmptyOrNullString())));
	}
	
	@Test
	public void testVirtualDependencies() throws FatalError, SQLException {
		List<Rule> resultRules = sut.config.rules;
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE3.A")
				.withPotentialDependants(hasItems(
						new TableField("PUBLIC.TABLE5.A1"),
						new TableField("PUBLIC.TABLE5.A2")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE4.B")
				.withPotentialDependants(hasItems(
						new TableField("PUBLIC.TABLE5.B1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE5.A1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE6.A1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE5.B1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE6.B1")))));
		// these are not intended but the program cannot know that:
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE6.A1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE5.A1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE6.B1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE5.B1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE6.A2")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE5.A2")))));
	}
	
	@Test
	public void testPartialDependencies() throws FatalError, SQLException {
		List<Rule> resultRules = sut.config.rules;
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE7.C")
				.withPotentialDependants(hasItems(
						new TableField("PUBLIC.TABLE9.C1"),
						new TableField("PUBLIC.TABLE9.C2")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE8.D")
				.withPotentialDependants(hasItems(
						new TableField("PUBLIC.TABLE9.D1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE9.C1")
				.withDependants(contains(new TableField("PUBLIC.TABLE10.C1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE9.D1")
				.withDependants(contains(new TableField("PUBLIC.TABLE10.D1")))));
		assertThat(resultRules, hasItem(rule().forAttribute("PUBLIC.TABLE9.C2")
				.withDependants(contains(new TableField("PUBLIC.TABLE10.C2")))));
		// the former false guesses should not be generated now
		assertThat(resultRules, not(hasItem(rule().forAttribute("PUBLIC.TABLE10.C1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE9.C1"))))));
		assertThat(resultRules, not(hasItem(rule().forAttribute("PUBLIC.TABLE10.D1")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE9.D1"))))));
		assertThat(resultRules, not(hasItem(rule().forAttribute("PUBLIC.TABLE10.C2")
				.withPotentialDependants(contains(new TableField("PUBLIC.TABLE9.C2"))))));
	}

	@Test
	public void testConfigFileOutputAsExpected() throws FatalError, SQLException, IOException {
		File outputFile = File.createTempFile("config-out-", null);
		outputFile.delete();
		try {
			sut.writeNewConfigToFile(outputFile.getPath());
			// -- start printf debugging --
//			for (String line : Files.readAllLines(outputFile.toPath(), Charset.defaultCharset()))
//				System.out.println(line);
			// -- end printf debugging --
			try (InputStream expectedFileStream = getClass().getResourceAsStream(
					getClass().getSimpleName() + "-config-output.txt");
					InputStreamReader expectedStreamReader =
							new InputStreamReader(expectedFileStream);
					BufferedReader expectedReader =
							new BufferedReader(expectedStreamReader);
					BufferedReader actualReader = Files.newBufferedReader(
							outputFile.toPath(), Charset.defaultCharset())) {
				String expectedLine, actualLine = "";
				while ((expectedLine = expectedReader.readLine()) != null) {
					actualLine = actualReader.readLine();
					assertThat(actualLine, not(is(nullValue())));
					assertThat(actualLine.split("[\n\r]", 2)[0],
							equalTo(expectedLine.split("[\n\r]", 2)[0]));
				}
				actualLine = actualReader.readLine();
				assertThat(actualLine, is(nullValue()));
			}
		} finally {
			outputFile.delete();
		}
	}
}
