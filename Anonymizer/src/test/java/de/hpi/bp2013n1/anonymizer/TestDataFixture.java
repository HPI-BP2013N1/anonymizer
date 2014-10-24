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


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.h2.tools.RunScript;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class TestDataFixture implements AutoCloseable {
	Connection originalDbConnection;
	Connection transformationDbConnection;
	Connection destinationDbConnection;
	Config config;
	Scope scope;
	
	public TestDataFixture() throws IOException, DependantWithoutRuleException, ClassNotFoundException, SQLException  {
		readConfigAndScope();
		createDbConnections();
	}
	
	public TestDataFixture(Config config, Scope scope)
			throws ClassNotFoundException, IOException, SQLException {
		this.config = config;
		this.scope = scope;
		createDbConnections();
	}

	public TestDataFixture(Connection originalDbConnection,
			Connection destinationDbConnection,
			Connection transformationDbConnection) throws IOException, DependantWithoutRuleException {
		readConfigAndScope();
		this.originalDbConnection = originalDbConnection;
		this.destinationDbConnection = destinationDbConnection;
		this.transformationDbConnection = transformationDbConnection;
	}

	private void readConfigAndScope() throws IOException, DependantWithoutRuleException {
		config = new Config();
		config.readFromURL(getConfigURL());
		scope = new Scope();
		scope.readFromURL(getScopeURL());
	}

	/**
	 * Template method which provides the URL to a scope file resource.
	 * @return
	 */
	protected URL getScopeURL() {
		return AnonymizerCompleteTest.class.getResource("testscope.txt");
	}

	/**
	 * Template method which provides the URL to a config file resource.
	 * @return
	 */
	protected URL getConfigURL() {
		return TestDataFixture.class.getResource("test-h2-config.txt");
	}

	private void createDbConnections() throws IOException,
			ClassNotFoundException, SQLException {
		originalDbConnection = DatabaseConnector.connect(config.originalDB);
		transformationDbConnection = DatabaseConnector.connect(config.transformationDB);
		destinationDbConnection = DatabaseConnector.connect(config.destinationDB);
	}
	
	public void populateDatabases() throws DatabaseUnitException, IOException, SQLException {
		populateDatabases(config.schemaName);
	}

	public void populateDatabases(String schemaName) 
			throws DatabaseUnitException, IOException, SQLException {
		for (InputStream ddlStream : getDDLs()) {
			executeDdlScript(ddlStream, originalDbConnection);
		}
		for (InputStream ddlStream : getDDLs()) {
			// cannot do this in the loop above because
			// the resources InputStreams cannot be reset
			executeDdlScript(ddlStream, destinationDbConnection);
		}
		for (InputStream ddlStream : getTransformationDDLs()) {
			executeDdlScript(ddlStream, transformationDbConnection);
		}
		InputStream originalDataSet = getOriginalDataSet();
		if (originalDataSet != null)
			importData(originalDbConnection, schemaName, originalDataSet);
		InputStream transformationDataSet = getTransformationDataSet();
		if (transformationDataSet != null)
			importData(transformationDbConnection, schemaName, transformationDataSet);
	}

	/**
	 * Template method for datasets that should be inserted into the original database.
	 * @return
	 */
	protected InputStream getOriginalDataSet() {
		return TestDataFixture.class.getResourceAsStream("testdata.xml");
	}

	/**
	 * Template method for datasets that should be inserted into the 
	 * transformation database.
	 * @return
	 */
	protected InputStream getTransformationDataSet() {
		return TestDataFixture.class.getResourceAsStream("testpseudonyms.xml");
	}

	/**
	 * Template method for DDLs that should be applied to the original and
	 * destination database.
	 * @return
	 */
	protected Iterable<InputStream> getDDLs() {
		return Lists.newArrayList(
				TestDataFixture.class.getResourceAsStream("testschema.ddl.sql"));
	}

	/**
	 * Template method for DDLs that should be applied to the 
	 * transformation database.
	 * @return
	 */
	protected Iterable<InputStream> getTransformationDDLs() {
		return Lists.newArrayList(
				TestDataFixture.class.getResourceAsStream("testpseudonymsschema.ddl.sql"));
	}
	
	void setSchema() throws SQLException {
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

	private static void executeDdlScript(InputStream ddlStream, 
			Connection dbConnection) throws SQLException, IOException {
		try (InputStreamReader ddlReader = new InputStreamReader(ddlStream)) {
			RunScript.execute(dbConnection, ddlReader);
		}
	}

	private static void importData(Connection connection, String schemaName,
			InputStream dataSetFileStream) throws DatabaseUnitException, IOException,
			SQLException {
		IDatabaseConnection db = new DatabaseConnection(connection, schemaName);
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		IDataSet data = dataSetBuilder.build(dataSetFileStream);
		DatabaseOperation.CLEAN_INSERT.execute(db, data);
	}
	
	Anonymizer createAnonymizer() {
		Anonymizer anonymizer = new Anonymizer(config, scope);
		anonymizer.useDatabases(originalDbConnection, destinationDbConnection, 
				transformationDbConnection);
		return anonymizer;
	}
	
	IDataSet expectedDestinationDataSet() throws DataSetException {
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		return dataSetBuilder.build(TestDataFixture.class
				.getResourceAsStream("transformed-testdata.xml"));
	}
	
	protected IDataSet actualDestinationDataSet() throws DatabaseUnitException, SQLException {
		IDatabaseConnection destination = new DatabaseConnection(
				destinationDbConnection, config.schemaName);
		return destination.createDataSet();
	}

	void closeConnections() throws SQLException {
		try {
			originalDbConnection.close();
		} finally {
			try {
				transformationDbConnection.close();
			} finally {
				destinationDbConnection.close();
			}
		}
	}

	@Override
	public void close() throws SQLException {
		closeConnections();
	}

	public void assertExpectedEqualsActualDataSet()
			throws DatabaseUnitException, SQLException, DataSetException {
		IDataSet actualDataSet = actualDestinationDataSet();
		IDataSet expectedDataSet = expectedDestinationDataSet();
		org.dbunit.Assertion.assertEquals(expectedDataSet, actualDataSet);
	}

	public static Config makeStubConfig() {
		Config config = new Config();
		config.originalDB.url = "jdbc:h2:mem:";
		config.destinationDB.url = "jdbc:h2:mem:";
		config.transformationDB.url = "jdbc:h2:mem:";
		return config;
	}
}
