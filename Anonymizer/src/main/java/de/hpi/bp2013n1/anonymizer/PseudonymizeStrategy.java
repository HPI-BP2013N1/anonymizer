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
	Map<Rule, Map<String, String>> cachedTransformations = Maps.newHashMap();
	private Map<Rule, PseudonymsTableProxy> pseudonymTables = Maps.newHashMap();
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
			checkArgument(pseudonymsTable.schema == null
					|| !pseudonymsTable.schema.isEmpty());
			checkArgument(SQLTypes.isValidType(columnType.type));
			checkArgument(database != null);
			this.tableSpec = pseudonymsTable;
			this.columnType = columnType;
			this.database = database;
		}
		
		public TableField getTableSite() {
			return tableSpec;
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
						+ tableSpec.schemaTable() + " "
						+ "( " + OLDVALUE + " " + columnType.toSQLString() + " NOT NULL, "
						+ NEWVALUE + " " + columnType.toSQLString() + " NOT NULL, "
						+ "PRIMARY KEY(" + OLDVALUE + "))");
			} catch (SQLException e) {
				throw new TransformationTableCreationException(
						"Creation of pseudonyms table " + tableSpec.schemaTable()
						+ " failed.", e);
			}
		}
		
		public void createIfNotExists() throws SQLException, TransformationTableCreationException {
			if (!exists())
				create();
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
								(T) existingPseudonymsResultSet.getObject(1),
								(T) existingPseudonymsResultSet.getObject(2));
					}
				}
			}
			return existingPseudonyms;
		}
		
		public Map<String, String> fetchStrings() throws SQLException {
			HashMap<String, String> existingPseudonyms = new HashMap<>();
			try (PreparedStatement selectExistingPseudonymsStatement =
					database.prepareStatement(
							"SELECT TRIM(TRAILING ' ' FROM " + OLDVALUE + "), "
									+ "TRIM(TRAILING ' ' FROM " + NEWVALUE + ") "
									+ "FROM "
									+ tableSpec.schemaTable())) {
				try (ResultSet existingPseudonymsResultSet =
						selectExistingPseudonymsStatement.executeQuery()) {
					while (existingPseudonymsResultSet.next()) {
						existingPseudonyms.put(
								existingPseudonymsResultSet.getString(1),
								existingPseudonymsResultSet.getString(2));
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
				if (originalValue instanceof String || originalValue instanceof Character)
					selectStatement.setString(1, originalValue.toString());
				else
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

		public String fetchOneString(String originalValue) throws SQLException, TransformationKeyNotFoundException {
			try (PreparedStatement selectStatement = database.prepareStatement(
					"SELECT TRIM(TRAILING ' ' FROM " + NEWVALUE + ") "
							+ "FROM " + tableSpec.schemaTable()
							+ " WHERE " + OLDVALUE + " = ?")) {
				selectStatement.setString(1, originalValue.toString());
				try (ResultSet resultSet = selectStatement.executeQuery()) {
					if (!resultSet.next())
						throw new TransformationKeyNotFoundException(
								"Could not find pseudonym for " + originalValue
								+ " in pseudonyms table " + tableSpec.schemaTable());
					return resultSet.getString(1);
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

		public void drop() throws SQLException {
			try (PreparedStatement dropStatement = database.prepareStatement(
					"DROP TABLE " + tableSpec.schemaTable())) {
				dropStatement.executeUpdate();
			}
		}
	}

	public PseudonymizeStrategy(Anonymizer anonymizer, Connection origDB,
			Connection translateDB) throws SQLException {
		super(anonymizer, origDB, translateDB);
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
		TableField originTableField = rule.getTableField();
		ColumnDatatypeDescription originTableFieldDatatype;
		PseudonymsTableProxy pseudonymsTable;
		try {
			originTableFieldDatatype =
				ColumnDatatypeDescription.fromMetaData(
						originTableField,
						originalDatabase);
			pseudonymsTable = getPseudonymsTableFor(originTableField, originTableFieldDatatype);
			pseudonymsTable.createIfNotExists();
		} catch (SQLException e1) {
			throw new TransformationTableCreationException("Could not prepare "
					+ "the creation of a pseudonyms table due to SQL errors", e1);
		}
		pseudonymTables.put(rule, pseudonymsTable);
		try {
			boolean isStringAttribute = SQLTypes.isCharacterType(originTableFieldDatatype.type);
			String distinctValuesQuery = distinctValuesQuery(rule, isStringAttribute);
			String countDistinctValuesQuery = countDistinctValuesQuery(distinctValuesQuery);

			int numberOfDistinctValues;
			try (Statement statement = originalDatabase.createStatement();
					ResultSet countDistinctValuesResultSet =
							statement.executeQuery(countDistinctValuesQuery)) {
				countDistinctValuesResultSet.next();
				numberOfDistinctValues = countDistinctValuesResultSet.getInt(1);
			}
			
			@SuppressWarnings("rawtypes")
			Map existingPseudonyms;
			if (isStringAttribute)
				existingPseudonyms = pseudonymsTable.fetchStrings();
			else
				existingPseudonyms = pseudonymsTable.fetch();
			Set<Object> newValues = determinateNewValuesInDatabase(existingPseudonyms, distinctValuesQuery);
			List<String> randomValues = new PseudonymGenerator().
					createNewPseudonyms(rule, originTableFieldDatatype, numberOfDistinctValues);
			randomValues.removeAll(existingPseudonyms.values());

			Map<Object, String> newMapping =
					PseudonymGenerator.createNewRandomMap(newValues, randomValues);
			pseudonymsTable.insertNewPseudonyms(newMapping);
		} catch (SQLException e) {
			throw new TransformationKeyCreationException(
					"An SQL error occurred when creating pseudonyms.", e);
		}
	}

	private PseudonymsTableProxy getPseudonymsTableFor(
			TableField originTableField,
			ColumnDatatypeDescription originTableFieldDatatype) {
		return new PseudonymsTableProxy(
				getPseudonymsTableSite(originTableField),
				originTableFieldDatatype, transformationDatabase);
	}

	public PseudonymsTableProxy getPseudonymsTableFor(Rule rule)
			throws SQLException {
		if (pseudonymTables.containsKey(rule))
			return pseudonymTables.get(rule);
		TableField originTableField = rule.getTableField();
		ColumnDatatypeDescription originTableFieldDatatype;
		originTableFieldDatatype =
				ColumnDatatypeDescription.fromMetaData(
						originTableField,
						originalDatabase);
		PseudonymsTableProxy pseudonymsTableForRule =
				getPseudonymsTableFor(originTableField, originTableFieldDatatype);
		pseudonymTables.put(rule, pseudonymsTableForRule);
		return pseudonymsTableForRule;
	}
	
	private Set<Object> determinateNewValuesInDatabase(
			@SuppressWarnings("rawtypes") Map existingPseudonyms,
			String distinctValuesQuery) throws SQLException {
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
	
	private static final String DISTINCT_VALUES_ALIAS = "distinctValues";

	private String distinctValuesQuery(Rule rule, boolean isStringValue) {
		TableField originTableField = rule.getTableField();
		StringBuilder distinctValuesQueryBuilder = new StringBuilder();
		String distinctValuesQueryForOneColumn =
				"(select distinct %s " + DISTINCT_VALUES_ALIAS + " from %s)";
		if (isStringValue)
			distinctValuesQueryForOneColumn = distinctValuesQueryForOneColumn
			.replaceFirst("%s", "TRIM(TRAILING ' ' FROM %s)");
		distinctValuesQueryBuilder.append(String.format(
				distinctValuesQueryForOneColumn,
				originTableField.column, originTableField.schemaTable()));
		
		for ( TableField dependant : rule.getDependants() ){
			distinctValuesQueryBuilder.append(" union ");
			distinctValuesQueryBuilder.append(String.format(
					distinctValuesQueryForOneColumn,
					dependant.column, dependant.schemaTable()));
		}
		
		return distinctValuesQueryBuilder.toString();
	}

	private String countDistinctValuesQuery(Rule rule, boolean isStringAttribute) {
		return String.format(
				"select count (%s) from (%s)",
				DISTINCT_VALUES_ALIAS,
				distinctValuesQuery(rule, isStringAttribute));
	}

	private String countDistinctValuesQuery(String distinctValuesQuery) {
		return String.format(
				"select count (%s) from (%s)",
				DISTINCT_VALUES_ALIAS,
				distinctValuesQuery);
	}

	public static String pseudonymsTableName(TableField originalTableField) {
		 return originalTableField.getTable() + "_" + originalTableField.getColumn();
	}

	public static TableField getPseudonymsTableSite(TableField transformedField) {
		return new TableField(pseudonymsTableName(transformedField), null,
				transformedField.getSchema());
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
						requiredLength, rule.getAdditionalInfo());
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
				randomValues = createIntegers(numberOfDistinctValues, requiredLength);
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

		public ArrayList<String> createIntegers(int cnt, int requiredLength) {
			ArrayList<String> values = new ArrayList<String>();
			for (int i = 0; i < cnt; i++) {
				values.add(createInteger(i, requiredLength));
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

		public String createInteger(int seed, int requiredLength) {
			int offset = seed;
			char[] value = new char[requiredLength];
			for (int i = 0; i < requiredLength; i++) {
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
		return Lists.newArrayList(transform(
				oldValue == null ? null : oldValue.toString(),
				rule, row));
	}
	

	public String transform(String oldValue, Rule rule, ResultSetRowReader row)
			throws SQLException, TransformationKeyNotFoundException {
		if (oldValue == null)
			return null;
		if ((oldValue.replaceAll(" +$", "").length() == 0))
			return "";
		
		Map<String, String> cachedPseudonyms = cachedTransformations.get(rule);
		if (cachedPseudonyms == null) {
			return getPseudonymsTableFor(rule).fetchOneString(oldValue);
		} else {
			String result = cachedPseudonyms.get(
					oldValue.replaceAll(" +$", ""));
			if (result == null)
				return getPseudonymsTableFor(rule).fetchOne(oldValue);
			return result;
		}
	}

	public void fetchTranslations(Collection<Rule> pseudonymizationRules)
			throws SQLException {
		cachedTransformations.keySet().retainAll(pseudonymizationRules);
		pseudonymizationRules = new ArrayList<>(pseudonymizationRules);
		pseudonymizationRules.removeAll(cachedTransformations.keySet());
		for (Rule rule : pseudonymizationRules) {
			cachedTransformations.put(rule,
					getPseudonymsTableFor(rule).fetchStrings());
		}
	}

	@Override
	public void prepareTableTransformation(TableRuleMap affectedColumnEntries) throws SQLException {
		fetchTranslations(affectedColumnEntries.getRules());
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
		if (rule.getAdditionalInfo().length() != 0) {
			if (!SQLTypes.isCharacterType(type)) {
				logger.severe("Prefix only supported for CHARACTER and VARCHAR fields. Skipping");
				return false;
			}
			if (rule.getAdditionalInfo().length() > length) {
				logger.severe("Provided default value is longer than maximum field length of " + length + ". Skipping");
				return false;
			}

			try (Statement stmt = originalDatabase.createStatement();
					ResultSet rs = stmt.executeQuery(
							countDistinctValuesQuery(rule, true))) {
				rs.next();
				int count = rs.getInt(1);
				if (rule.getAdditionalInfo().length()
						+ Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS))
						> length) {
					logger.severe("Provided prefix is too long to pseudonymize. "
							+ "prefix: " + rule.getAdditionalInfo().length()
							+ " required: "
							+ Math.ceil(Math.log10(count)/Math.log10(NUMBER_OF_AVAILABLE_CHARS))
							+ " allowed: " + length + ". Skipping");
					return false;
				}
			} catch (SQLException e) {
				throw new RuleValidationException(
						"Could not count distinct values in column "
								+ rule.getTableField().schemaTable() + "."
								+ rule.getTableField().column
								+ ": " + e.getMessage());
			}
		}
		return true;
	}
}
