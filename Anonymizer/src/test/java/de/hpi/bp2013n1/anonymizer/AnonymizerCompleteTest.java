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


import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AnonymizerCompleteTest {

	private static final String NEW_ZIPCODE = "48454";
	private static final String NEW_NAME = "Hans";
	private static final String NEW_SURNAME = "Müller";
	static File logFile;
	static TreeMap<String, String> cinemaAddressPseudonyms;
	static TreeMap<String, String> cinemaCompanyPseudonyms;
	static TreeMap<String, String> visitorSurnamePseudonyms;
	static TreeMap<String, String> visitorZipcodePseudonyms;
	static TreeMap<String, String> moviePseudonyms;
	static TestDataFixture testData;

	@BeforeClass
	public static void anonymizeTestDbAndCreateConnections() throws Exception {
		testData = new TestDataFixture();
		testData.populateDatabases();
		loadOldPseudonyms();
		addNewRow();
		logFile = File.createTempFile("anonymizer-test-config-output", null);
		logFile.delete();
		Anonymizer.setUpLogging(logFile.getPath());
		Anonymizer anonymizer = testData.createAnonymizer();
		anonymizer.run();
	}

	@Before
	public void setSchema() throws SQLException {
		testData.setSchema();
	}

	private static void loadOldPseudonyms() throws SQLException {
		cinemaAddressPseudonyms = new TreeMap<>();
		cinemaCompanyPseudonyms = new TreeMap<>();
		visitorSurnamePseudonyms = new TreeMap<>();
		visitorZipcodePseudonyms = new TreeMap<>();
		moviePseudonyms = new TreeMap<>();
		testData.setSchema();
		fetchPseudonyms(cinemaAddressPseudonyms, "CINEMA_ADDRESS");
		fetchPseudonyms(cinemaCompanyPseudonyms, "CINEMA_COMPANY");
		fetchPseudonyms(visitorSurnamePseudonyms, "VISITOR_SURNAME");
		fetchPseudonyms(visitorZipcodePseudonyms, "VISITOR_ZIPCODE");
		fetchPseudonyms(moviePseudonyms, "VISIT_MOVIE");
	}

	private static void fetchPseudonyms(Map<String, String> pseudonymMap,
			String pseudonymTable) throws SQLException {
		String selectQuery = "SELECT OLDVALUE, NEWVALUE FROM %s";
		try (PreparedStatement fetchStatement = testData.transformationDbConnection
				.prepareStatement(String.format(selectQuery, pseudonymTable))) {
			try (ResultSet result = fetchStatement.executeQuery()) {
				while (result.next()) {
					pseudonymMap.put(result.getString("OLDVALUE"),
							result.getString("NEWVALUE"));
				}
			}
		}
	}

	@AfterClass
	public static void closeConnectionsAndDeleteFiles() throws SQLException {
		testData.setSchema();
		removeNewRows();
		try {
			testData.closeConnections();
		} finally {
			logFile.delete();
		}
	}

	private static void addNewRow() throws SQLException {
		try (PreparedStatement insertStatement = testData.originalDbConnection
				.prepareStatement("INSERT INTO VISITOR (NAME, SURNAME, BIRTHDATE, ZIPCODE, ADDRESS) "
						+ "VALUES (?, ?, '19740114', ?, 'Am Haus 738')")) {
			insertStatement.setString(1, NEW_NAME);
			insertStatement.setString(2, NEW_SURNAME);
			insertStatement.setString(3, NEW_ZIPCODE);
			if (insertStatement.executeUpdate() != 1) {
				fail("Could not insert " + NEW_NAME + " " + NEW_SURNAME);
			}
		}
	}

	private static void removeNewRows() throws SQLException {
		try (PreparedStatement deleteOriginalStatement = 
				testData.originalDbConnection
				.prepareStatement("DELETE FROM VISITOR WHERE NAME = ? AND SURNAME = ? "
						+ "AND BIRTHDATE = '19740114'");
				PreparedStatement deleteSurnameTranslationStatement = 
						testData.transformationDbConnection
						.prepareStatement("DELETE FROM VISITOR_SURNAME WHERE OLDVALUE = ?");
				PreparedStatement deleteZipCodeTranslationStatement = 
						testData.transformationDbConnection
						.prepareStatement("DELETE FROM VISITOR_ZIPCODE WHERE OLDVALUE = ?")) {
			deleteOriginalStatement.setString(1, NEW_NAME);
			deleteOriginalStatement.setString(2, NEW_SURNAME);
			if (deleteOriginalStatement.executeUpdate() < 1) {
				System.err.println("Could not delete " + NEW_NAME + " "
						+ NEW_SURNAME);
			}
			deleteSurnameTranslationStatement.setString(1, NEW_SURNAME);
			if (deleteSurnameTranslationStatement.executeUpdate() < 1) {
				System.err.println("Could not delete pseudonym for "
						+ NEW_SURNAME);
			}
			deleteZipCodeTranslationStatement.setString(1, NEW_ZIPCODE);
			if (deleteZipCodeTranslationStatement.executeUpdate() < 1) {
				System.err.println("Could not delete pseudonym for "
						+ NEW_ZIPCODE);
			}
		}
	}

	@Test
	public void retainedFormerPseudonyms() throws SQLException {
		assertPseudonymsUnchanged(cinemaAddressPseudonyms, "CINEMA_ADDRESS");
		assertPseudonymsUnchanged(cinemaCompanyPseudonyms, "CINEMA_COMPANY");
		assertPseudonymsUnchanged(visitorSurnamePseudonyms, "VISITOR_SURNAME");
		assertPseudonymsUnchanged(visitorZipcodePseudonyms, "VISITOR_ZIPCODE");
		assertPseudonymsUnchanged(moviePseudonyms, "VISIT_MOVIE");
	}

	private void assertPseudonymsUnchanged(Map<String, String> oldPseudonyms,
			String pseudonymTable) throws SQLException {
		TreeMap<String, String> newPseudonyms = new TreeMap<>();
		fetchPseudonyms(newPseudonyms, pseudonymTable);
		for (Map.Entry<String, String> oldPseudonym : oldPseudonyms.entrySet()) {
			assertEquals("Previous pseudonyms should not be changed",
					oldPseudonym.getValue(),
					newPseudonyms.get(oldPseudonym.getKey()));
		}
	}

	@Test
	public void createdNewPseudonymForNewRow() throws SQLException {
		try (PreparedStatement selectPseudonymStatement = 
				testData.transformationDbConnection
				.prepareStatement("SELECT OLDVALUE, NEWVALUE FROM VISITOR_SURNAME WHERE OLDVALUE = ?");
				PreparedStatement selectDestinationRowStatement = 
						testData.destinationDbConnection
						.prepareStatement("SELECT NAME, ZIPCODE FROM VISITOR WHERE SURNAME = ?")) {
			selectPseudonymStatement.setString(1, NEW_SURNAME);
			try (ResultSet pseudonymResultSet = selectPseudonymStatement
					.executeQuery()) {
				assertTrue("Translation for the new surname should exist",
						pseudonymResultSet.next());
				assertEquals("New translation should match the new surname",
						NEW_SURNAME, pseudonymResultSet.getString("OLDVALUE"));
				selectDestinationRowStatement.setString(1,
						pseudonymResultSet.getString("NEWVALUE"));
				try (ResultSet destinationResultSet = selectDestinationRowStatement
						.executeQuery()) {
					assertTrue("Anonymized new row should exist",
							destinationResultSet.next());
					assertEquals(
							"Anonymized row should match the original row where values have not changed",
							NEW_NAME, destinationResultSet.getString("NAME"));
					assertNotEquals(
							"Pseudonomized new value should differ from original",
							NEW_ZIPCODE,
							destinationResultSet.getString("ZIPCODE"));
				}
			}
		}
	}

	@Test
	public void checkEqualRowCounts() throws SQLException {
		String schema = "ORIGINAL";
		for (String tableName : Lists.newArrayList("VISITOR", "CINEMA", 
				"GREATMOVIES", "VISIT")) {
			String schemaTable = schema + "." + tableName;
			String countQuery = "SELECT COUNT(*) FROM " + schemaTable;
			try (PreparedStatement selectOriginalCountStatement = 
					testData.originalDbConnection
					.prepareStatement(countQuery);
					PreparedStatement selectAnonymizedCountStatement = 
							testData.destinationDbConnection
							.prepareStatement(countQuery)) {
				try (ResultSet originalCountResultSet = selectOriginalCountStatement
						.executeQuery();
						ResultSet anonymizedCountResultSet = selectAnonymizedCountStatement
								.executeQuery()) {
					originalCountResultSet.next();
					anonymizedCountResultSet.next();
					assertThat(
							"Anonymized table should have same number of rows as original table"
									+ " (" + schemaTable + ")",
									anonymizedCountResultSet.getInt(1),
									equalTo(originalCountResultSet.getInt(1)));
				}
			}
		}
	}

	private IDatabaseConnection getDbUnitConnection()
			throws DatabaseUnitException {
		IDatabaseConnection db = new DatabaseConnection(
				testData.destinationDbConnection, "ORIGINAL");
		return db;
	}
	
	@Test
	public void checkUniformDistributionDeletions() throws DatabaseUnitException, SQLException {
		IDatabaseConnection db = getDbUnitConnection();
		IDataSet data = testData.expectedDestinationDataSet();
		ITable productSalesTable = data.getTable("PRODUCTSALES");
		ITable actualTable = db.createTable("PRODUCTSALES");
		org.dbunit.Assertion.assertEquals(productSalesTable, actualTable);
	}

	@Test
	public void checkForeignKeyDependantDeletions() throws DatabaseUnitException, SQLException {
		IDatabaseConnection db = getDbUnitConnection();
		IDataSet data = testData.expectedDestinationDataSet();
		for (String tableName : Lists.newArrayList("PRODUCTBUYER", "BUYERDETAILS")) {
			ITable expectedTable = data.getTable(tableName);
			ITable actualTable = db.createTable(tableName);
			org.dbunit.Assertion.assertEquals(expectedTable, actualTable);
		}
	}
}
