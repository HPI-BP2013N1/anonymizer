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


import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hpi.bp2013n1.anonymizer.db.ColumnDatatypeDescription;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class PseudonymizeStrategy extends TransformationStrategy {

	static Logger logger = Logger.getLogger(PseudonymizeStrategy.class.getName());
	static final int NUMBER_OF_AVAILABLE_CHARS = 2 * 26 + 10;
	char[] shuffledCharPool = shuffledChars();
	char[] shuffledNumbersPool = shuffledNumberArray();
	HashMap<String, TreeMap<String, String>> cachedTransformations;
	PreparedStatement newDBStmt;
	
	public static class PseudonymsTableProxy {
		private TableField tableSpec;
		private Connection database;
		private ColumnDatatypeDescription columnType;
		static String OLDVALUE = "OLDVALUE";
		static String NEWVALUE = "NEWVALUE";

		public PseudonymsTableProxy(TableField pseudonymsTable,
				ColumnDatatypeDescription columnType, Connection database) {
			checkArgument(!Strings.isNullOrEmpty(pseudonymsTable.table));
			checkArgument(!Strings.isNullOrEmpty(pseudonymsTable.schema));
			checkArgument(SQLTypes.isValidType(columnType.type));
			checkArgument(database != null);
			this.tableSpec = pseudonymsTable;
			this.columnType = columnType;
			this.database = database;
		}
		
		public boolean exists() throws SQLException {
			try (ResultSet tableResult = database.getMetaData().getTables(
					null, tableSpec.schema, tableSpec.table, new String[] { "TABLE" })) {
				return tableResult.next();
			}
		}

		public void create() throws TransformationTableCreationException {
			try (Statement createTableStatement = database.createStatement()) {
				createTableStatement.executeUpdate("CREATE TABLE "
						+ tableSpec.table + " "
						+ "( " + OLDVALUE + " " + SQLTypes.getTypeName(columnType.type) + " NOT NULL, "
						+ NEWVALUE + " " + SQLTypes.getTypeName(columnType.type) + " NOT NULL, "
						+ "PRIMARY KEY(" + OLDVALUE + "))");
			} catch (SQLException e) {
				throw new TransformationTableCreationException(
						"Creation of pseudonyms table failed.");
			}
		}

		@SuppressWarnings("unchecked")
		public <T> Map<T, T> fetch() throws SQLException {
			HashMap<T, T> existingPseudonyms = new HashMap<>();
			try (PreparedStatement selectExistingPseudonymsStatement =
					database.prepareStatement(
							"SELECT " + OLDVALUE + ", " + NEWVALUE + " FROM "
									+ tableSpec.schemaTable())) {
				try (ResultSet existingPseudonymsResultSet =
						selectExistingPseudonymsStatement.executeQuery()) {
					while (existingPseudonymsResultSet.next()) {
						existingPseudonyms.put(
								(T) existingPseudonymsResultSet.getObject(OLDVALUE),
								(T) existingPseudonymsResultSet.getObject(NEWVALUE));
					}
				}
			}
			return existingPseudonyms;
		}
		
		@SuppressWarnings("unchecked")
		public <T> T fetchOne(T originalValue) throws SQLException, TransformationKeyNotFoundException {
			try (PreparedStatement selectStatement = database.prepareStatement(
					"SELECT " + NEWVALUE + " FROM " + tableSpec.schemaTable()
					+ " WHERE " + OLDVALUE + " = ?")) {
				selectStatement.setObject(1, originalValue);
				try (ResultSet resultSet = selectStatement.executeQuery()) {
					if (!resultSet.next())
						throw new TransformationKeyNotFoundException(
								"Could not find pseudonym for " + originalValue
								+ " in pseudonyms table " + tableSpec.schemaTable());
					return (T) resultSet.getObject(1);
				}
			}
		}

		public <T1, T2> void insertNewPseudonyms(Map<T1, T2> newMapping)
				throws TransformationKeyCreationException, SQLException {
			int maximalLength = columnType.length;
			String insertQuery = "INSERT INTO " + tableSpec.schemaTable()
					+ " VALUES (?,?)";
			try (PreparedStatement newDBStmt = database.prepareStatement(insertQuery)) {
				for (T1 newValue : newMapping.keySet()) {
					if (newValue == null) {
						continue;
					}
					T2 pseudonym = newMapping.get(newValue);
					if (newValue instanceof String
							&& newValue.toString().length() > maximalLength)
						throw new TransformationKeyCreationException(
								"Could not create keys for " + tableSpec.schemaTable()
								+ ". Original value too long. Check config File.");
					newDBStmt.setObject(1, newValue);
					newDBStmt.setObject(2, pseudonym);
					newDBStmt.addBatch();
				}
				newDBStmt.executeBatch();
			}
			database.commit();
		}

		public ColumnDatatypeDescription getColumnType() {
			return columnType;
		}
	}

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
		PseudonymsTableProxy pseudonymsTable;
		try {
			originTableFieldDatatype =
				ColumnDatatypeDescription.fromMetaData(
						originTableField,
						originalDatabase);
			pseudonymsTable = new PseudonymsTableProxy(
					getPseudonymsTable(originTableField),
					originTableFieldDatatype, transformationDatabase);
			if (!pseudonymsTable.exists()) {
				pseudonymsTable.create();
			}
		} catch (SQLException e1) {
			throw new TransformationTableCreationException("Could not prepare "
					+ "the creation of a pseudonyms table due to SQL errors", e1);
		}
		try {
			String distinctValuesQuery = distinctValuesQuery(rule,
					originTableField);
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
			
			Map<Object, Object> existingPseudonyms = pseudonymsTable.fetch();
			Set<Object> newValues = determinateNewValuesInDatabase(existingPseudonyms, distinctValuesQuery);
			List<String> randomValues = new PseudonymGenerator().
					createNewPseudonyms(rule, originTableFieldDatatype, numberOfDistinctValues);
			randomValues.removeAll(existingPseudonyms.values());

			Map<Object, String> newMapping =
					PseudonymGenerator.createNewRandomMap(newValues, randomValues);
			pseudonymsTable.insertNewPseudonyms(newMapping);
		} catch (SQLException e) {
			e.printStackTrace();
			while ((e = e.getNextException()) != null)
				e.printStackTrace();
		}
	}

	private Set<Object> determinateNewValuesInDatabase(
			Map<Object, Object> existingPseudonyms, String distinctValuesQuery) throws SQLException {
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
		return newValues;
	}

	private String distinctValuesQuery(Rule rule, TableField originTableField) {
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
		
		return distinctValuesQueryBuilder.toString();
	}

	public static TableField getPseudonymsTable(TableField transformedField) {
		// TODO: move code from TableField.translationTableName to PseudonymizeStrategy
		return new TableField(transformedField.translationTableName(), null,
				transformedField.schema);
	}
	
	public static class PseudonymGenerator {
		private char[] shuffledCharPool = shuffledChars();
		private char[] shuffledNumbersPool = shuffledNumberArray();

		public List<String> createNewPseudonyms(Rule rule,
				ColumnDatatypeDescription originTableFieldDatatype,
				int numberOfDistinctValues) throws ColumnTypeNotSupportedException {
			ArrayList<String> randomValues = null;
			int requiredLength = (int) Math.ceil((Math.log(numberOfDistinctValues)
					/ Math.log(NUMBER_OF_AVAILABLE_CHARS)));
			switch (originTableFieldDatatype.type) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
				randomValues = createRandomPseudonyms(numberOfDistinctValues,
						requiredLength, rule.additionalInfo);
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
				randomValues = createIntegers(numberOfDistinctValues, originTableFieldDatatype.length);
				break;
			default:
				throw new ColumnTypeNotSupportedException(
						"Column type is not supported for pseudonymization: "
								+ originTableFieldDatatype);
			}
			return randomValues;
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
		
		public static <T1, T2> Map<T1, T2> createNewRandomMap(Set<T1> newValues,
				List<T2> randomValues) {
			Map<T1, T2> newMapping = Maps.newHashMap();
			Random random = new Random();
			for (T1 newValue : newValues) {
				if (newValue == null)
					continue;
				T2 pseudonym = randomValues.remove(random.nextInt(
						randomValues.size()));
				newMapping.put(newValue, pseudonym);
			}
			return newMapping;
		}
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
		// TODO: refactor this with PseudonymsTableProxy
		fetchTranslations(collectTranslationTableNamesFor(affectedColumnEntries));
	}

	private ArrayList<String> collectTranslationTableNamesFor(TableRuleMap rules) {
		ArrayList<String> result = new ArrayList<String>();
		for (Rule configRule : rules.getRules()) {
			result.add(configRule.tableField.schemaQualifiedTranslationTableName());
		}
		return result;
	}

	private boolean isSupportedType(int type) {
		return SQLTypes.isCharacterType(type) || SQLTypes.isIntegerType(type);
	}
	
	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException {
		// check for valid type
		if (!isSupportedType(type)) {
			logger.severe("Pseudonymisation only supports CHAR, VARCHAR and INTEGER fields");
			return false;
		}

		// check for prefix is valid
		if (rule.additionalInfo.length() != 0) {
			if (!SQLTypes.isCharacterType(type)) {
				logger.severe("Prefix only supported for CHARACTER and VARCHAR fields. Skipping");
				return false;
			}
			if (rule.additionalInfo.length() > length) {
				logger.severe("Provided default value is longer than maximum field length of " + length + ". Skipping");
				return false;
			}

			try (Statement stmt = originalDatabase.createStatement();
				ResultSet rs = stmt.executeQuery(
						selectDistinctValuesFromParentAndDependentColumns(rule))) {
				rs.next();
				int count = rs.getInt(1);
				if (rule.additionalInfo.length()
						+ Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS))
						> length) {
					logger.severe("Provided prefix is too long to pseudonymize. "
							+ "prefix: " + rule.additionalInfo.length()
							+ " required: "
							+ Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS))
							+ " allowed: " + length + ". Skipping");
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

	private String selectDistinctValuesFromParentAndDependentColumns(Rule rule) {
		// TODO: why is distinctValuesQuery(rule, originTableField) not used here?
		TableField tableField = rule.tableField;
		// rename columns to onlyDistinctValuesXYZ so you can make a union
		StringBuilder query = new StringBuilder(
				"select count (distinct onlyDistinctValuesXYZ) from (");
		query.append("(select distinct ").append(tableField.column)
		.append(" onlyDistinctValuesXYZ from ").append(tableField.schemaTable())
		.append(")");
		for (TableField tf : rule.dependants) {
			query.append(" union (select distinct ").append(tf.column)
			.append(" onlyDistinctValuesXYZ from ").append(tf.schemaTable())
			.append(")");
		}
		query.append(")");
		return query.toString();
	}
}
