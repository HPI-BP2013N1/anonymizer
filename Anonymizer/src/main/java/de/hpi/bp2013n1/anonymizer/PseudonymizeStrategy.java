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


import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.db.ColumnDatatypeDescription;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.AnonymizerUtils;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class PseudonymizeStrategy extends TransformationStrategy {

	private static final String INTEGER = "INTEGER";
	private static final String VARCHAR = "VARCHAR";
	private static final String CHARACTER = "CHARACTER";
	private static final String CHAR = "CHAR";
	static final int NUMBER_OF_AVAILABLE_CHARS = 2 * 26 + 10;
	char[] shuffledCharPool = shuffledChars();
	char[] shuffledNumbersPool = shuffledNumberArray();
	HashMap<String, TreeMap<String, String>> cachedTransformations;
	PreparedStatement newDBStmt;

	public PseudonymizeStrategy(Anonymizer anonymizer, Connection origDB, 
			Connection translateDB) throws SQLException {
		super(anonymizer, origDB, translateDB);
		cachedTransformations = new HashMap<>();
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules)
			throws TransformationKeyCreationException, TransformationTableCreationException, ColumnTypeNotSupportedException {
		for (Rule configRule : rules)
			setUpTransformation(configRule);
	}
	
	public void setUpTransformation(Rule rule)
			throws TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException {
		TableField originTableField = rule.tableField;
		ColumnDatatypeDescription originTableFieldDatatype;
		try {
			originTableFieldDatatype = 
				AnonymizerUtils.getColumnDatatypeDescription(
						originTableField, 
						originalDatabase);
			if (!translationTableExists(originTableField)) {
				createTranslationTable(originTableField.schemaQualifiedTranslationTableName(), 
						originTableFieldDatatype);
			}
		} catch (SQLException e1) {
			throw new TransformationTableCreationException(e1.getMessage());
		}
		try {
			StringBuilder distinctValuesQueryBuilder = new StringBuilder();
			String distinctValuesQueryForOneColumn = 
					"(select distinct %s distinctValues from %s)";
			distinctValuesQueryBuilder.append(String.format(
					distinctValuesQueryForOneColumn,
					originTableField.column, originTableField.schemaTable()));
			
			for ( TableField dependant : rule.dependants ){
				distinctValuesQueryBuilder.append(" union ");
				distinctValuesQueryBuilder.append(String.format(
						distinctValuesQueryForOneColumn,
						dependant.column, dependant.schemaTable()));
			}
			
			String distinctValuesQuery = distinctValuesQueryBuilder.toString();
			String countDistinctValuesQuery = String.format(
					"select count (distinctValues) from (%s)", 
					distinctValuesQuery);

			int numberOfDistinctValues;
			try (Statement statement = originalDatabase.createStatement();
					ResultSet countDistinctValuesResultSet = 
							statement.executeQuery(countDistinctValuesQuery)) {
				countDistinctValuesResultSet.next();
				numberOfDistinctValues = countDistinctValuesResultSet.getInt(1);
			}
			
			HashMap<Object, Object> existingPseudonyms = fetchExistingPseudonyms(
					originTableField.schemaQualifiedTranslationTableName());

			HashSet<Object> newValues = new HashSet<Object>();
			try (Statement statement = originalDatabase.createStatement();
					ResultSet distinctValuesResultSet = 
							statement.executeQuery(distinctValuesQuery)) {
				while (distinctValuesResultSet.next()) {
					Object originalValue = distinctValuesResultSet.getObject(1);
					if (!existingPseudonyms.containsKey(originalValue)) {
						newValues.add(originalValue);
					}
				}
			}

			ArrayList<String> randomValues = createNewPseudonyms(rule,
					originTableFieldDatatype, numberOfDistinctValues);
			randomValues.removeAll(existingPseudonyms.values());

			String insertQuery = "INSERT INTO " + originTableField.schemaQualifiedTranslationTableName() 
					+ " VALUES (?,?)";
			newDBStmt = transformationDatabase.prepareStatement(insertQuery);

			Random random = new Random();
			int maximalLength = originTableFieldDatatype.length;
			for (Object newValue : newValues) {
				if (newValue == null) {
					continue;
				}
				String pseudonym = randomValues.remove(
						random.nextInt(randomValues.size()));
				if (newValue.toString().length() > maximalLength)
					throw new TransformationKeyCreationException(
							"Could not create keys for " 
									+ originTableField.schemaQualifiedTranslationTableName() 
									+ ". Original value too long. Check config File.");
				newDBStmt.setObject(1, newValue);
				newDBStmt.setObject(2, pseudonym);
				newDBStmt.addBatch();
			}
			newDBStmt.executeBatch();
			transformationDatabase.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			while ((e = e.getNextException()) != null)
				e.printStackTrace();
		}
	}

	private ArrayList<String> createNewPseudonyms(Rule rule,
			ColumnDatatypeDescription originTableFieldDatatype,
			int numberOfDistinctValues) throws ColumnTypeNotSupportedException {
		ArrayList<String> randomValues = null;
		int requiredLength = (int) Math.ceil((Math.log(numberOfDistinctValues) 
				/ Math.log(NUMBER_OF_AVAILABLE_CHARS)));
		switch (originTableFieldDatatype.typename) {
		case CHARACTER:
		case CHAR:
		case VARCHAR:
			randomValues = createRandomPseudonyms(numberOfDistinctValues,
					requiredLength, rule.additionalInfo);
			break;
		case INTEGER:
			randomValues = createIntegers(numberOfDistinctValues, originTableFieldDatatype.length);
			break;
		default:
			throw new ColumnTypeNotSupportedException(
					"Column type is not supported for pseudonymization: " 
							+ originTableFieldDatatype);
		}
		return randomValues;
	}

	private HashMap<Object, Object>
	fetchExistingPseudonyms(String translationTableName) throws SQLException {
		HashMap<Object, Object> existingPseudonyms = new HashMap<>();
		try (PreparedStatement selectExistingPseudonymsStatement = 
				transformationDatabase.prepareStatement(
						"SELECT OLDVALUE, NEWVALUE FROM " 
								+ translationTableName)) {
			try (ResultSet existingPseudonymsResultSet = 
					selectExistingPseudonymsStatement.executeQuery()) {
				while (existingPseudonymsResultSet.next()) {
					existingPseudonyms.put(
							existingPseudonymsResultSet.getObject("OLDVALUE"),
							existingPseudonymsResultSet.getObject("NEWVALUE"));
				}
			}
		}
		return existingPseudonyms;
	}

	private boolean translationTableExists(TableField translatedField) 
			throws SQLException {
		ResultSet tableResultSet = 
				transformationDatabase.getMetaData().getTables(
						null,
						translatedField.schema,
						translatedField.translationTableName(),
						new String[] { "TABLE" });
		try {
			return tableResultSet.next(); // next returns true if a row is available
		} finally {
			tableResultSet.close();
		}
	}

	private void createTranslationTable(String translationTableName, 
			ColumnDatatypeDescription fieldDatatype)
					throws TransformationTableCreationException {
		try (Statement createTableStatement = transformationDatabase.createStatement()) {
			createTableStatement.execute("CREATE TABLE " 
					+ translationTableName + " "
					+ "(oldValue " + fieldDatatype + " NOT NULL, "
					+ "newValue " + fieldDatatype + " NOT NULL, "
					+ "PRIMARY KEY(oldvalue))");
		} catch (SQLException e) {
			throw new TransformationTableCreationException("Creating translation keys failed.");
		}
	}

	public ArrayList<String> createRandomPseudonyms(int cnt, int length, String prefix) {
		checkArgument(cnt <= Math.pow(NUMBER_OF_AVAILABLE_CHARS, length),
				"Cannot produce %d pseudonyms of length %d", cnt, length);
		ArrayList<String> pseudonyms = new ArrayList<String>();
		for (int i = 0; i < cnt; i++) {
			pseudonyms.add(createPseudonym(i, length, prefix));
		}
		return pseudonyms;
	}

	public ArrayList<String> createIntegers(int cnt, int length) {
		ArrayList<String> values = new ArrayList<String>();
		for (int i = 0; i < cnt; i++) {
			values.add(createInteger(i, length));
		}
		return values;
	}

	public String createPseudonym(int seed, int length, String prefix) {
		// The following does not look really random and if you put the
		// generated values with increasing offset into a List, you will see
		// a pattern. The actual randomness comes from the shuffledCharPool.
		// This algorithm makes sure that for NUMBER_OF_AVAILABLE_CHARS^length
		// invocations with incremented offsets each time a unique pseudonym
		// is returned.
		int offset = seed;
		char[] value = new char[length];
		for (int i = length - 1; i >= 0; i--) {
			value[i] = shuffledCharPool[(offset % NUMBER_OF_AVAILABLE_CHARS)];
			offset = offset / NUMBER_OF_AVAILABLE_CHARS;
		}
		return prefix + String.copyValueOf(value);
	}

	public String createInteger(int seed, int length) {
		int offset = seed;
		char[] value = new char[length];
		for (int i = 0; i < length; i++) {
			value[i] = shuffledNumbersPool[(offset % 10)];
			offset = offset / 10;
		}
		return String.copyValueOf(value);
	}

	@Override
	public List<String> transform(Object oldValue, Rule rule, ResultSetRowReader row) 
			throws SQLException, TransformationKeyNotFoundException {
		checkArgument(oldValue instanceof String || oldValue == null, 
				getClass() + " should transform "
				+ oldValue + " but can only operate on Strings.");
		// TODO: check if this works out because we broke the assumption that
		// even integer columns will be put in as Strings
		return Lists.newArrayList(transform((String) oldValue, rule, row));
	}
	

	public String transform(String oldValue, Rule rule, ResultSetRowReader row) 
			throws SQLException, TransformationKeyNotFoundException {
		if (oldValue == null)
			return null;
		if ((oldValue.replaceAll(" +$", "").length() == 0))
			return "";
		
		String result = cachedTransformations.get(
				rule.tableField.schemaQualifiedTranslationTableName()).get(
						oldValue.replaceAll(" +$", ""));
		
		if (result != null)
			return result;
		else {	
			throw new TransformationKeyNotFoundException("Value not found");
		}
	}

	/**
	 * Attention: removes elements from given argument  
	 * @param translationTableNames
	 * @throws SQLException 
	 */
	public void fetchTranslations(ArrayList<String> translationTableNames)
			throws SQLException {
		cachedTransformations.keySet().retainAll(translationTableNames);
		translationTableNames.removeAll(cachedTransformations.keySet());
		for (String tableName : translationTableNames) {
			String query = "SELECT * FROM " + tableName;
			try (Statement translateDbStatement = transformationDatabase.createStatement();
					ResultSet cacheSet = translateDbStatement.executeQuery(query)) {
				TreeMap<String, String> cachedTranslations = new TreeMap<>();
				cachedTransformations.put(tableName, cachedTranslations);

				while (cacheSet.next()) {
					cachedTranslations.put(
							cacheSet.getString("oldValue").replaceAll(" +$", ""), 
							cacheSet.getString("newValue"));
				}
			}
		}
	}

	@Override
	public void prepareTableTransformation(TableRuleMap affectedColumnEntries) throws SQLException {
		fetchTranslations(collectTranslationTableNamesFor(affectedColumnEntries));
	}

	private ArrayList<String> collectTranslationTableNamesFor(TableRuleMap rules) {
		ArrayList<String> result = new ArrayList<String>();
		for (Rule configRule : rules.getRules()) {
			result.add(configRule.tableField.schemaQualifiedTranslationTableName());
		}
		return result;
	}

	@Override
	public boolean isRuleValid(Rule rule, String typename, int length,
			boolean nullAllowed) throws RuleValidationException {
		// check for valid type
		if (!(typename.equals("CHARACTER") 
				|| typename.equals("CHAR")
				|| typename.equals("VARCHAR") 
				|| typename.equals("INTEGER"))) {
			System.err.println("ERROR: Pseudonymisation only supports CHARACTER, VARCHAR and INTEGER fields");
			return false;
		}

		// check for prefix is valid
		if (rule.additionalInfo.length() != 0) {
			if (!(typename.equals("CHARACTER")
					|| typename.equals("CHAR")
					|| typename.equals("VARCHAR"))) {
				System.err.println("ERROR: Prefix only supported for CHARACTER and VARCHAR fields. Skipping");
				return false;
			}
			if (rule.additionalInfo.length() > length) {						
				System.err.println("ERROR: Provided default value is longer than maximum field length of " + length + ". Skipping");
				return false;
			}


			try (Statement stmt = originalDatabase.createStatement();
				ResultSet rs = stmt.executeQuery(
						"SELECT COUNT(DISTINCT " + rule.tableField.column + ") "
								+ "FROM " + rule.tableField.schemaTable())) {
				rs.next();
				int count = rs.getInt(1);
				if (rule.additionalInfo.length() 
						+ Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS)) 
						> length) {
					System.err.println("ERROR: Provided default value is too long to pseudonymize. prefix: " + rule.additionalInfo.length() 
							+ " required: " + Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS)) + " allowed: " + length + ". Skipping");
					return false;
				}
			} catch (SQLException e) {
				throw new RuleValidationException(
						"Could not count distinct values in column " 
								+ rule.tableField.schemaTable() + "." 
								+ rule.tableField.column 
								+ ": " + e.getMessage());
			}
		}
		return true;
	}
}
