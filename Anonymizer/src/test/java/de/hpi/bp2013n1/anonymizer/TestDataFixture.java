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

import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class TestDataFixture implements AutoCloseable {
	Connection originalDbConnection;
	Connection transformationDbConnection;
	Connection destinationDbConnection;
	Config config;
	Scope scope;
	
	public TestDataFixture() throws Exception {
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
			Connection transformationDbConnection) throws IOException, Exception {
		readConfigAndScope();
		this.originalDbConnection = originalDbConnection;
		this.destinationDbConnection = destinationDbConnection;
		this.transformationDbConnection = transformationDbConnection;
	}

	private void readConfigAndScope() throws Exception, IOException {
		config = new Config();
		config.readFromURL(TestDataFixture.class.getResource("test-h2-config.txt"));
		scope = new Scope();
		scope.readFromURL(AnonymizerCompleteTest.class.getResource("testscope.txt"));
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
		executeDdlScript("testschema.ddl.sql", originalDbConnection);
		executeDdlScript("testschema.ddl.sql", destinationDbConnection);
		executeDdlScript("testpseudonymsschema.ddl.sql", transformationDbConnection);
		importData(originalDbConnection, schemaName, "testdata.xml");
		importData(transformationDbConnection, schemaName,
				"testpseudonyms.xml");
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

	private static void executeDdlScript(String scriptFileName,
			Connection dbConnection) throws SQLException, IOException {
		try (InputStream ddlStream = TestDataFixture.class
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
		IDataSet data = dataSetBuilder.build(TestDataFixture.class
				.getResourceAsStream(dataSetFilename));
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
	
	IDataSet actualDestinationDataSet() throws DatabaseUnitException, SQLException {
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

	public static Config makeStubConfig() {
		Config config = new Config();
		config.originalDB.url = "jdbc:h2:mem:";
		config.destinationDB.url = "jdbc:h2:mem:";
		config.transformationDB.url = "jdbc:h2:mem:";
		return config;
	}
}
