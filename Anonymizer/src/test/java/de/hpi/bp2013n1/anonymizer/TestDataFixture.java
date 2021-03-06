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
import java.util.logging.Logger;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.h2.tools.RunScript;

import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

public abstract class TestDataFixture implements AutoCloseable {
	
	private static Logger logger = Logger.getLogger(TestDataFixture.class.getName());

	private static void executeDdlScript(InputStream ddlStream,
			Connection dbConnection) throws IOException {
		try (InputStreamReader ddlReader = new InputStreamReader(ddlStream)) {
			RunScript.execute(dbConnection, ddlReader);
		} catch (SQLException e) {
			logger.severe("Error during execution of DDL script: " + e.getMessage());
		}
	}

	private static void importData(Connection connection, String schemaName,
			InputStream dataSetFileStream) throws DatabaseUnitException,
			IOException, SQLException {
		IDatabaseConnection db = new DatabaseConnection(connection, schemaName);
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		IDataSet data = dataSetBuilder.build(dataSetFileStream);
		DatabaseOperation.CLEAN_INSERT.execute(db, data);
	}

	public static Config makeStubConfig() {
		Config config = new Config();
		config.originalDB.url = "jdbc:h2:mem:";
		config.destinationDB.url = "jdbc:h2:mem:";
		config.transformationDB.url = "jdbc:h2:mem:";
		return config;
	}

	protected Connection originalDbConnection;
	protected Connection transformationDbConnection;
	protected Connection destinationDbConnection;
	
	public Connection getOriginalDbConnection() {
		return originalDbConnection;
	}

	public Connection getTransformationDbConnection() {
		return transformationDbConnection;
	}

	public Connection getDestinationDbConnection() {
		return destinationDbConnection;
	}

	protected Config config;
	protected Scope scope;

	public Config getConfig() {
		return config;
	}

	public Scope getScope() {
		return scope;
	}

	protected TestDataFixture() {
	}

	public TestDataFixture(Config config, Scope scope)
			throws ClassNotFoundException, IOException, SQLException, DependantWithoutRuleException, MalformedException {
		this.config = config != null ? config : loadConfig();
		this.scope = scope != null ? scope : loadScope();
		createDbConnections();
	}

	/**
	 * Template method which provides the URL to a scope file resource.
	 * 
	 * @return
	 */
	protected abstract URL getScopeURL();

	/**
	 * Template method which provides the URL to a config file resource.
	 * 
	 * @return
	 */
	protected abstract URL getConfigURL();

	protected void createDbConnections() throws IOException,
			ClassNotFoundException, SQLException {
		originalDbConnection = DatabaseConnector.connect(config.originalDB);
		transformationDbConnection = DatabaseConnector
				.connect(config.transformationDB);
		destinationDbConnection = DatabaseConnector
				.connect(config.destinationDB);
	}

	public void populateDatabases() throws DatabaseUnitException, IOException,
			SQLException {
		populateDatabases(config.schemaName);
	}

	public void populateDatabases(String schemaName)
			throws DatabaseUnitException, IOException, SQLException {
		createOriginalDatabaseTables();
		createDestinationDatabaseTables();
		createTransformationDatabaseTables();
		populateTables(schemaName);
	}

	public void createOriginalDatabaseTables() throws SQLException, IOException {
		for (InputStream ddlStream : getDDLs()) {
			executeDdlScript(ddlStream, originalDbConnection);
		}
	}

	public void createDestinationDatabaseTables() throws SQLException, IOException {
		for (InputStream ddlStream : getDDLs()) {
			executeDdlScript(ddlStream, destinationDbConnection);
		}
	}

	public void createTransformationDatabaseTables() throws SQLException,
			IOException {
		for (InputStream ddlStream : getTransformationDDLs()) {
			executeDdlScript(ddlStream, transformationDbConnection);
		}
	}

	public void populateTables(String schemaName) throws DatabaseUnitException,
			IOException, SQLException {
		InputStream originalDataSet = getOriginalDataSet();
		if (originalDataSet != null)
			importData(originalDbConnection, schemaName, originalDataSet);
		InputStream transformationDataSet = getTransformationDataSet();
		if (transformationDataSet != null)
			importData(transformationDbConnection, schemaName,
					transformationDataSet);
	}
	
	public void tearDownDatabases() throws SQLException, IOException {
		tearDownDatabases(config.schemaName);
	}
	
	public void tearDownDatabases(String schemaName) throws SQLException,
			IOException {
		for (InputStream tearDownStream : getTearDownSQL()) {
			executeDdlScript(tearDownStream, originalDbConnection);
		}
		for (InputStream tearDownStream : getTearDownSQL()) {
			executeDdlScript(tearDownStream, destinationDbConnection);
		}
		for (InputStream tearDownStream : getTransformationTearDownSQL()) {
			executeDdlScript(tearDownStream, transformationDbConnection);
		}
	}

	/**
	 * Template method for datasets that should be inserted into the original
	 * database.
	 * 
	 * @return
	 */
	protected abstract InputStream getOriginalDataSet();

	/**
	 * Template method for datasets that should be inserted into the
	 * transformation database.
	 * 
	 * @return
	 */
	protected abstract InputStream getTransformationDataSet();

	/**
	 * Template method for DDLs that should be applied to the original and
	 * destination database.
	 * 
	 * @return
	 */
	protected abstract Iterable<InputStream> getDDLs();

	/**
	 * Template method for sql streams that contain statements which drop
	 * tables which were created by the DDLs in the original and destination
	 * database.
	 * 
	 * @return
	 */
	protected abstract Iterable<InputStream> getTearDownSQL();

	/**
	 * Template method for DDLs that should be applied to the transformation
	 * database.
	 * 
	 * @return
	 */
	protected abstract Iterable<InputStream> getTransformationDDLs();

	/**
	 * Template method for sql stream that drops tables created by the
	 * transformation database DDL.
	 * 
	 * @return
	 */
	protected abstract Iterable<InputStream> getTransformationTearDownSQL();

	public void setSchema() throws SQLException {
		try (Statement s = originalDbConnection.createStatement()) {
			s.execute(SQLHelper.setSchemaStatement(config.schemaName, originalDbConnection));
		}
		// originalDbConnection.setSchema(config.schemaName);
		try (Statement s = transformationDbConnection.createStatement()) {
			s.execute(SQLHelper.setSchemaStatement(config.schemaName, transformationDbConnection));
		}
		// transformationDbConnection.setSchema(config.schemaName);
		try (Statement s = destinationDbConnection.createStatement()) {
			s.execute(SQLHelper.setSchemaStatement(config.schemaName, destinationDbConnection));
		}
		// destinationDbConnection.setSchema(config.schemaName);
	}

	public Anonymizer createAnonymizer() {
		Anonymizer anonymizer = new Anonymizer(config, scope);
		anonymizer.useDatabases(originalDbConnection, destinationDbConnection,
				transformationDbConnection);
		return anonymizer;
	}

	protected abstract IDataSet expectedDestinationDataSet()
			throws DataSetException;

	protected IDataSet actualDestinationDataSet() throws DatabaseUnitException,
			SQLException {
		IDatabaseConnection destination = new DatabaseConnection(
				destinationDbConnection, config.schemaName);
		return destination.createDataSet();
	}

	protected void closeConnections() throws SQLException {
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

	protected void readConfigAndScope() throws IOException,
			DependantWithoutRuleException, MalformedException {
		config = loadConfig();
		scope = loadScope();
	}
	
	public Config loadConfig() throws IOException, DependantWithoutRuleException, MalformedException {
		Config config = new Config();
		config.readFromURL(getConfigURL());
		return config;
	}
	
	public Scope loadScope() throws IOException {
		Scope scope = new Scope();
		scope.readFromURL(getScopeURL());
		return scope;
	}

}
