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


import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.Anonymizer.FatalError;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;

public class ForeignKeyDeletionsByConfigRuleTest {

	private TestDataFixture testData;
	private File logFile;
	
	@Before
	public void createTestData() throws ClassNotFoundException, IOException, DependantWithoutRuleException, SQLException, DatabaseUnitException {
		testData = new TestSpecificTestDataFixture(this);
		testData.populateDatabases();
		testData.setSchema();
	}

	@Before
	public void prepareLogFile() throws IOException {
		logFile = File.createTempFile("anonymizer-test-config-output", null);
		logFile.delete();
		Anonymizer.setUpLogging(logFile.getPath());
	}
	
	@After
	public void closeDatabaseConnections() throws SQLException {
		testData.closeConnections();
	}

	@After
	public void deleteLogFile() {
		logFile.delete();
	}

	@Test
	public void weakDependentRowsAreDeleted() throws FatalError, DatabaseUnitException, SQLException {
		Anonymizer anonymizer = testData.createAnonymizer();
		anonymizer.run();
		testData.assertExpectedEqualsActualDataSet();
	}
}
