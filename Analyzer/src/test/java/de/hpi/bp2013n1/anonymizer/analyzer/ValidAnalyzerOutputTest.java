package de.hpi.bp2013n1.anonymizer.analyzer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.Anonymizer;
import de.hpi.bp2013n1.anonymizer.Anonymizer.FatalError;
import de.hpi.bp2013n1.anonymizer.StandardTestDataFixture;
import de.hpi.bp2013n1.anonymizer.TestDataFixture;
import de.hpi.bp2013n1.anonymizer.shared.Config;

/**
 * Asserts that the Analyzer output can direclty be used by the Anonymizer.
 *
 */
public class ValidAnalyzerOutputTest {
	
	TestDataFixture testData;
	Analyzer analyzer;
	Anonymizer anonymizer;

	@Before
	public void setUp() throws Exception {
		Config config = new Config();
		config.readFromURL(ValidAnalyzerOutputTest.class.getResource(
				"AnalyzerTestConfig.txt"));
		testData = new StandardTestDataFixture(config, null);
		testData.populateDatabases();
		analyzer = new Analyzer(testData.getOriginalDbConnection(),
				testData.getConfig(), testData.getScope());
		anonymizer = testData.createAnonymizer();
	}

	@Test
	public void test() throws IOException,
			de.hpi.bp2013n1.anonymizer.analyzer.Analyzer.FatalError {
		File outputConfig = File.createTempFile("analyzer-output", ".txt");
		outputConfig.delete();
		outputConfig.deleteOnExit();
		assumeThat(testData.getConfig().getRules().get(0).getDependants(),
				is(empty()));
		analyzer.run(outputConfig.getPath());
		// this should have modified the config
		assertThat(testData.getConfig().getRules().get(0).getDependants(),
				hasSize(greaterThan(0)));
		try {
			anonymizer.run();
		} catch (FatalError e) {
			fail("Anonymizer encountered FatalError with Analyzer output: "
					+ e.getMessage());
		}
	}

}
