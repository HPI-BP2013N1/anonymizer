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


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.ColumnTypeNotSupportedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.FetchPseudonymsFailedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.PreparationFailedExection;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.TransformationFailedException;
import de.hpi.bp2013n1.anonymizer.db.BatchOperation;
import de.hpi.bp2013n1.anonymizer.db.ColumnDatatypeDescription;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.AnonymizerUtils;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

public class Anonymizer {
	
	private Config config;
	private Scope scope;
	private Connection originalDatabase, anonymizedDatabase, transformationDB;
	private ArrayList<TransformationStrategy> transformationStrategies = new ArrayList<>();
	private TreeMap<String, TransformationStrategy> strategyByClassName = new TreeMap<>();
	private ArrayList<TableRuleMap> tableRuleMaps;
	private final int LOG_INTERVAL = 1000;
	
	private static final Logger anonymizerLogger = Logger.getLogger(Anonymizer.class.getName());
	private static FileHandler logFileHandler;
	private static SimpleFormatter logFormatter;
	private int currentTableNumber;
	private RowRetainService retainService;
	
	public static class TableNotInScopeException extends Exception {
		private static final long serialVersionUID = -4527921975005958468L;
	}
	
	public static class FatalError extends Exception {
		private static final long serialVersionUID = -6431519862013506163L;
	}
	
	/**
	 * Initializes Anonymizer
	 * Connects databases specified in config
	 * Creates strategies with database connections 
	 * 
	 * @param config Config object generated from config file.
	 * @param scope Scope object generated from scope file.
	 * @throws SQLException
	 */
	public Anonymizer(Config config, Scope scope) {
		this.config = config;
		this.scope = scope;
	}

	public RowRetainService getRetainService() {
		return retainService;
	}

	public void connectAndRun() throws FatalError {
		if (!connectDatabases()) {
			throw new FatalError();
		}
		run();
	}

	protected void run() throws FatalError {
		try {
			loadAndInstantiateStrategies();
		} catch (ClassNotFoundException e) {
			anonymizerLogger.severe("Could not load strategy: " + e.getMessage());
			throw new FatalError();
		}
		// getConstructor
		catch (NoSuchMethodException e) {
			anonymizerLogger.severe("Strategy is missing the required constructor: "
					+ e.getMessage());
			throw new FatalError();
		} catch (SecurityException e) {
			anonymizerLogger.severe("Could not access strategy constructor: "
					+ e.getMessage());
			throw new FatalError();
		} 
		// newInstance
		catch (InstantiationException | IllegalAccessException 
				| IllegalArgumentException | InvocationTargetException e) {
			anonymizerLogger.severe("Could not create strategy: " + e.getMessage());
			throw new FatalError();
		}
		// not a TransformationStrategy
		catch (ClassCastException e) {
			// error message has already been emitted in loadAndInstanciateStrategy
			throw new FatalError();
		}
	
		try {
			if (checkLengths() == 0)
				try {
					anonymize();
				} catch (TransformationTableCreationException e) {
					anonymizerLogger.severe("Could not create pseudonyms table: "
							+ e.getMessage());
					throw new FatalError();
				} catch (TransformationKeyCreationException e) {
					anonymizerLogger.severe("Could not create pseudonyms: "
							+ e.getMessage());
				} catch (FetchPseudonymsFailedException e) {
					anonymizerLogger.severe("Could not retrieve pseudonyms: "
							+ e.getMessage());
				} catch (ColumnTypeNotSupportedException e) {
					anonymizerLogger.severe("An anonymization strategy does not "
							+ "support the type of column to which the strategy "
							+ "should be applied: " + e.getMessage());
					throw new FatalError();
				} catch (PreparationFailedExection e) {
					anonymizerLogger.severe(e.getMessage());
					throw new FatalError();
				}
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while checking whether all "
					+ "values will fit into their column: "
					+ e.getMessage());
			throw new FatalError();
		}
	}
	
	public void loadAndInstantiateStrategies() throws ClassNotFoundException,
			NoSuchMethodException, InstantiationException,
			IllegalAccessException, InvocationTargetException, 
			ClassCastException {
		for (Rule rule : config.rules) {
			rule.strategy = config.strategyMapping.get(rule.strategy);
			loadAndInstanciateStrategy(rule.strategy);	
		}
	}
	
	protected void loadAndInstanciateStrategy(String strategyClassName)
					throws ClassNotFoundException, NoSuchMethodException, 
					InstantiationException, IllegalAccessException, 
					InvocationTargetException, ClassCastException {
		try {
			TransformationStrategy strategy = 
					TransformationStrategy.loadAndCreate(strategyClassName, 
							this, originalDatabase, transformationDB);
			transformationStrategies.add(strategy);
			strategyByClassName.put(strategyClassName, strategy);
		} catch (ClassCastException e) {
			anonymizerLogger.severe(
					String.format("%s is not a valid transformation class",
					strategyClassName));
			throw e;
		}
	}

	private boolean connectDatabases() {

		try {
			originalDatabase = DatabaseConnector.connect(this.config.originalDB);
			anonymizedDatabase = DatabaseConnector.connect(this.config.destinationDB);
			transformationDB = DatabaseConnector.connect(this.config.transformationDB);

		} catch (ClassNotFoundException | SQLException e) {
			anonymizerLogger.severe("Could not connect to Databases. ");
			e.printStackTrace();
			return false;
		}
		retainService = new RowRetainService(originalDatabase, transformationDB);
		return true;
	}
	
	protected void useDatabases(
			Connection originalDbConnection,
			Connection destinationDbConnection,
			Connection transformationDbConnection) {
		originalDatabase = originalDbConnection;
		anonymizedDatabase = destinationDbConnection;
		transformationDB = transformationDbConnection;
		retainService = new RowRetainService(originalDatabase, transformationDB);
	}

	/**
	 * Loads Config, and scopes. Then starts the anonymization
	 * 
	 * @param args
	 *            [0] = path to intermediary config file, args[1] = path to scope file
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {			
		if (args.length != 3) {
			System.err.println("Expected 3 Arguments\n" +
					"1. : path to intermediary config file, \n" + 
					"2. : path to scope file,\n" + 
					"3. : desired name of logfile");
			return;
		}		
		
		setUpLogging(args[2]);
				
		Config config = new Config();
		Scope scope = new Scope();
		try {
			anonymizerLogger.info("Reading config file.");
			config.readFromFile(args[0]);
		} catch (IOException e) {
			anonymizerLogger.severe("Could not read from config file: " 
					+ e.getMessage());
			return;
		} catch (Exception e) {
			anonymizerLogger.severe("Reading config file failed: " 
					+ e.getMessage());
			return;
		}
		try {
			anonymizerLogger.info("Reading scope file");
			scope.readFromFile(args[1]);
		} catch (IOException e) {
			anonymizerLogger.severe("Reading scope file failed: "
					+ e.getMessage());
			return;
		}
		Anonymizer anon = new Anonymizer(config, scope);
		try {
			anon.connectAndRun();
		} catch (FatalError e) {
			anonymizerLogger.severe("Cannot recover from previous errors, exiting.");
		}
	}

	protected static void setUpLogging(String logFilename) throws IOException {
		System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] - %4$s: %5$s (%2$s)%n");
		Logger logger = Logger.getLogger("de.hpi.bp2013n1");
		logFileHandler = new FileHandler(logFilename);
		
		logFormatter = new SimpleFormatter();		
						
		logger.addHandler(logFileHandler);
		logFileHandler.setFormatter(logFormatter);
	}
	
	public int checkLengths() throws SQLException {
		int numberOfErrors = 0;
		for (Rule rule : config.rules) {
			TableField tableField = rule.tableField;
			String query = "select count (distinct onlyDistinctValuesXYZ) anzahl from (";
			query = query + "(select distinct " + tableField.column + " onlyDistinctValuesXYZ from " + tableField.schemaTable() + ")";
			for (TableField tf : rule.dependants) {
				query = query + " union (select distinct " + tf.column + " onlyDistinctValuesXYZ from " + tf.schemaTable() + ")";
			}
			query = query + ")";
			
			Statement originalStatement = originalDatabase.createStatement();
			
			ResultSet resultSet = originalStatement.executeQuery(query);
			resultSet.next();
			int count = resultSet.getInt("anzahl");

			ColumnDatatypeDescription description = 
					AnonymizerUtils.getColumnDatatypeDescription(
							tableField, originalDatabase);
			if (rule.strategy.equals(PseudonymizeStrategy.class.getName())) {
				switch (description.typename) {
				// TODO: extract constants
				case "CHARACTER":
				case "CHAR":
					if (description.length < ((int) Math.ceil(Math.log(count) / Math.log(62)) + rule.additionalInfo.length())) {
						anonymizerLogger.severe("Too many values for pseudonymization of " + rule.tableField + ". Please check the config-file.");
						numberOfErrors++;
					}
					break;
				case "VARCHAR":
					if (description.length < ((int) Math.ceil(Math.log(count) / Math.log(62)) + rule.additionalInfo.length())) {
						anonymizerLogger.severe("Too many values for pseudonymization of " + rule.tableField + ". Please check the config-file.");			
						numberOfErrors++;
					}
					break;
				default:
					break;
				}
			}
		}
		return numberOfErrors;
	}

	/**
	 * Main anonymization method. Creates transformation tables in
	 * transformation database, then copies and/or anonymizes the data
	 * 
	 * @throws ColumnTypeNotSupportedException
	 * @throws TransformationTableCreationException
	 * @throws TransformationKeyCreationException
	 * @throws FetchPseudonymsFailedException
	 * @throws PreparationFailedExection 
	 * @throws SQLException
	 * @throws Exception
	 */
	public void anonymize() throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedExection {
		anonymizerLogger.info("Started anonymizing.");
		ArrayList<Constraint> constraints = disableAnonymizedDbConstraints();

		prepareTransformations();
		copyAndAnonymizeData();

		enableAnonymizedDbConstraints(constraints);
		anonymizerLogger.info("Finished: Anonymizing");
		for (TransformationStrategy strategy : transformationStrategies)
			strategy.printSummary();
	}

	private ArrayList<Constraint> disableAnonymizedDbConstraints() {
		boolean supportsDisableAllForeignKeys = 
				anonymizedDatabaseSupportsDisableAllForeignKeys();
		if (supportsDisableAllForeignKeys) {
			try (Statement disableStatement = anonymizedDatabase.createStatement()) {
				try {
					disableStatement.execute(SQLHelper.disableAllForeignKeys(anonymizedDatabase));
				} catch (SQLException e) {
					anonymizerLogger.warning(String.format(
							"Failed to disable foreign key constraints: %s",
							e.getMessage()));
				}
			} catch (SQLException e) {
				anonymizerLogger.warning(String.format(
						"An error occured about disabling a constraint: %s",
						e.getMessage()));
			}
			return null;
		} else {
			ArrayList<Constraint> constraints = findConstraints(anonymizedDatabase);
			disableConstraints(constraints, anonymizedDatabase);
			return constraints;
		}
	}

	private void enableAnonymizedDbConstraints(ArrayList<Constraint> constraints) {
		boolean supportsDisableAllForeignKeys = 
				anonymizedDatabaseSupportsDisableAllForeignKeys();
		if (supportsDisableAllForeignKeys) {
			try (Statement enableStatement = anonymizedDatabase.createStatement()) {
				try {
					enableStatement.execute(SQLHelper.enableAllForeignKeys(anonymizedDatabase));
				} catch (SQLException e) {
					anonymizerLogger.warning(String.format(
							"Failed to enable foreign key constraints: %s",
							e.getMessage()));
				}
			} catch (SQLException e) {
				anonymizerLogger.warning(String.format(
						"An error occured about enabling a constraint: %s",
						e.getMessage()));
			}
		} else {
			enableConstraints(constraints, anonymizedDatabase);
		}
	}

	private boolean anonymizedDatabaseSupportsDisableAllForeignKeys() {
		try {
			return SQLHelper
					.supportsDisableAllForeignKeys(anonymizedDatabase);
		} catch (SQLException e) {
			anonymizerLogger.warning(String.format(
					"Could not determine whether database supports disabling "
					+ "all foreign key constraints at once: %s", e.getMessage()));
			return false;
		}
	}
	
	private void copyAndAnonymizeData() {
		anonymizerLogger.info("Started copying data.");
		tableRuleMaps = AnonymizerUtils.createTableRuleMaps(config, scope);

		currentTableNumber = 0;
		for (TableRuleMap tableRuleMap : tableRuleMaps) {
			copyAndAnonymizeTable(tableRuleMap);
		}
		anonymizerLogger.info("Finished: Copying Data.");
	}

	private void copyAndAnonymizeTable(TableRuleMap tableRuleMap) {
		anonymizerLogger.info("Copying data from: " + tableRuleMap.tableName + 
				" (table " + (++currentTableNumber) + "/" + tableRuleMaps.size() + ").");
		// make sure target newDB is empty
		String qualifiedTableName = config.schemaName + "." + tableRuleMap.tableName;
		truncateTable(qualifiedTableName);
		ResultSetMetaData rsMeta;
		int columnCount = 0;
		int rowCount = countRowsInTable(qualifiedTableName);
		try (PreparedStatement selectStarStatement = originalDatabase.prepareStatement(
				"SELECT * FROM " + qualifiedTableName);
				ResultSet rs = selectStarStatement.executeQuery()) {
			try {
				rsMeta = rs.getMetaData();
				columnCount = rsMeta.getColumnCount();	
				
				for (TransformationStrategy strategy : transformationStrategies)
					strategy.prepareTableTransformation(
							tableRuleMap.filteredByStrategy(
									strategy.getClass().getName()));
				
			} catch (SQLException e) {
				anonymizerLogger.warning("Fetching rows failed: " + e.getMessage());
				e.printStackTrace();
				return;
			}

			copyAndAnonymizeRows(tableRuleMap, qualifiedTableName, rsMeta,
					columnCount, rowCount, rs);
		} catch (SQLException e) {
			anonymizerLogger.severe("Could not query table " 
					+ qualifiedTableName + ": " + e.getMessage());
		}
	}

	private void copyAndAnonymizeRows(TableRuleMap tableRuleMap,
			String qualifiedTableName, ResultSetMetaData rsMeta,
			int columnCount, int rowCount, ResultSet rs) {
		// prepared Statement for batch loading
		StringBuilder preparedSQLQueryBuilder = new StringBuilder();
		preparedSQLQueryBuilder.append("INSERT INTO " + config.schemaName + "." + tableRuleMap.tableName + " VALUES (");
		for (int j = 0; j < columnCount - 1; j++)
			preparedSQLQueryBuilder.append("?,");
		preparedSQLQueryBuilder.append("?)");			
		
		ResultSetRowReader rowReader = new ResultSetRowReader(rs);
		rowReader.setCurrentTable(tableRuleMap.tableName);
		rowReader.setCurrentSchema(config.schemaName);
		try (PreparedStatement anonymizedDatabaseStatement = anonymizedDatabase.prepareStatement(
				preparedSQLQueryBuilder.toString())) {
			int processedRowsCount = 0;
			while (!rs.isClosed() && rs.next()) { // for all rows
				copyAndAnonymizeRow(tableRuleMap, qualifiedTableName, rsMeta,
						columnCount, rowReader, anonymizedDatabaseStatement);
				processedRowsCount++;
				if ((processedRowsCount % config.batchSize) == 0) {
					BatchOperation.executeAndCommit(anonymizedDatabaseStatement);
				}
				
				if ((processedRowsCount % LOG_INTERVAL) == 0)
					System.out.format("Progress: %d/%d (%d %%)\r", processedRowsCount, rowCount, 100 * processedRowsCount / rowCount);
				
			}
			if ((processedRowsCount % LOG_INTERVAL) != 0)
				System.out.format("Progress: %d/%d (%d %%)\n", processedRowsCount, rowCount, 100 * processedRowsCount / rowCount);
			else
				System.out.format("\n");
			
			BatchOperation.executeAndCommit(anonymizedDatabaseStatement);
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while traversing table "
					+ qualifiedTableName + ": " + e.getMessage());
		}
	}

	private void copyAndAnonymizeRow(TableRuleMap tableRuleMap,
			String qualifiedTableName, ResultSetMetaData rsMeta,
			int columnCount, ResultSetRowReader rowReader,
			PreparedStatement anonymizedDatabaseStatement) throws SQLException {
		boolean retainRow = false;
		List<Iterable<?>> columnValues = new ArrayList<>(columnCount);
		for (int j = 1; j <= columnCount; j++) { // for all columns
			String columnName = rsMeta.getColumnName(j);
			ImmutableList<Rule> appliedRules = tableRuleMap.getRules(columnName); // check if column needs translation
			if (!appliedRules.isEmpty()) {
				// fetch translations
				Iterable<?> currentValues = Lists.newArrayList(rowReader.getObject(j));
				for (Rule configRule : appliedRules) {
					Iterable<Object> newValues = Collections.emptyList();
					for (Object intermediateValue : currentValues) {
						Iterable<?> transformationResults = anonymizeValue(
								intermediateValue, configRule, rowReader, j,
								columnName, tableRuleMap);
						newValues = Iterables.concat(newValues, 
								transformationResults);
					}
					if (!currentValues.iterator().hasNext()) {
						if (retainRow 
								|| retainService.currentRowShouldBeRetained(
										configRule.tableField.schema, 
										configRule.tableField.table,
										rowReader)) {
							// skip this transformation which deleted the tuple
							anonymizerLogger.info(
									"Not deleting a row in " 
											+ qualifiedTableName + " because it "
											+ "was previously marked to be "
											+ "retained.");
							retainRow = true;
							continue;
						}
						// if a Strategy returned an empty transformation, the
						// cross product of all column values will be empty,
						// the original tuple is lost
						return;
					}
					currentValues = newValues;
				}
				columnValues.add(currentValues); 
			} else {
				// if column doesn't have to be anonymized, take old value
				columnValues.add(Lists.newArrayList(rowReader.getObject(j))); 
			}
		}					
		try {
			addBatchInserts(anonymizedDatabaseStatement, columnValues);
		} catch (SQLException e) {
			anonymizerLogger.severe("Adding Statement :" 
					+ anonymizedDatabaseStatement + " failed: " 
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Compute the cross product of columnValues and add them to 
	 * anonymizedDatabaseStatement with {@link PreparedStatement.addBatch}.
	 * @param anonymizedDatabaseStatement
	 * @param columnValues
	 * @throws SQLException if calls to anonymizedDatabaseStatement fail 
	 */
	private void addBatchInserts(PreparedStatement anonymizedDatabaseStatement,
			List<Iterable<?>> columnValues) throws SQLException {
		addBatchInserts(anonymizedDatabaseStatement, columnValues, 1);
	}

	private void addBatchInserts(PreparedStatement anonymizedDatabaseStatement,
			List<Iterable<?>> columnValues, int columnIndex) throws SQLException {
		if (columnIndex > columnValues.size()) {
			anonymizedDatabaseStatement.addBatch();
		} else {
			for (Object columnValue : columnValues.get(columnIndex - 1)) {
				anonymizedDatabaseStatement.setObject(columnIndex, columnValue);
				addBatchInserts(anonymizedDatabaseStatement, columnValues, 
						columnIndex + 1);
			}
		}
	}

	private Iterable<?> anonymizeValue(Object currentValue,
			Rule configRule, ResultSetRowReader rowReader, int columnIndex,
			String columnName, TableRuleMap tableRules) throws SQLException {
		TransformationStrategy strategy;
		strategy = getStrategyFor(configRule);
		try {
			return strategy.transform(currentValue, configRule, rowReader);
		} catch (TransformationKeyNotFoundException e) {
			anonymizerLogger.log(Level.SEVERE,
					"Transformation value for \"" + 
							currentValue + "\" (from table " + 
					tableRules.tableName + "." + columnName + 
					") was not found in keys for " + 
					configRule.tableField + 
					". Used empty String instead.",
					e);
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while transforming value "
					+ currentValue + "\" (from table " + 
					tableRules.tableName + "." + columnName + 
					") : " + e.getMessage() + ". Using empty String instead.");
		} catch (TransformationFailedException e) {
			anonymizerLogger.severe(e.getMessage());
		}
		return Lists.newArrayList("");
	}

	private int countRowsInTable(String qualifiedTableName) {
		int rowCount = -1;
		try (Statement countStatement = originalDatabase.createStatement();
				ResultSet countResult = countStatement.executeQuery(
						"SELECT COUNT(*) FROM " + qualifiedTableName)) {
			countResult.next();
			rowCount = countResult.getInt(1);
		} catch (SQLException e) {
			anonymizerLogger.info("Could not retrieve row count for table "
					+ qualifiedTableName + " (error message follows), "
					+ "progress report will be awkward.");
			anonymizerLogger.warning(e.getMessage());
		}
		return rowCount;
	}

	private void truncateTable(String qualifiedTableName) {
		try (Statement truncateStatement = anonymizedDatabase.createStatement()) {
			truncateStatement.executeUpdate(
					SQLHelper.truncateTable(anonymizedDatabase, 
							qualifiedTableName));
		} catch (SQLException e) {
			anonymizerLogger.warning("Could not truncate table "
					+ qualifiedTableName + ": " + e.getMessage());
			anonymizerLogger.warning("A non-empty destination database "
					+ "can lead to subsequent fatal errors.");
		}
	}

	public TransformationStrategy getStrategyFor(Rule configRule) {
		return strategyByClassName.get(configRule.strategy);
	}

	private void prepareTransformations() throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedExection {
		anonymizerLogger.info("Preparing transformations.");
		Multimap<String, Rule> rulesByStrategy = ArrayListMultimap.create();
		for (Rule rule : config.rules) {
			if (!scope.tables.contains(rule.tableField.table)) {
				anonymizerLogger.warning("Table " + rule.tableField.table 
						+ " not in scope. Skipping dependants and continuing.");
				continue;
			}
			rulesByStrategy.put(rule.strategy, rule);
			for (TableField dependant : rule.dependants) {
				if (!scope.tables.contains(dependant.table)){
					anonymizerLogger.warning("Dependend table " + dependant.table 
							+ " not in scope. Skipping dependant and continuing.");
					continue;
				}
			}
		}
		for (String strategyName : rulesByStrategy.keySet()) {
			strategyByClassName.get(strategyName).setUpTransformation(
					rulesByStrategy.get(strategyName));
		}
		anonymizerLogger.info("Finished: preparing transformations.");
	}

	private ArrayList<Constraint> findConstraints(Connection connection) {
		ConstraintNameFinder finder = new ConstraintNameFinder(connection);
		ArrayList<Constraint> constraintList = new ArrayList<Constraint>();
		ArrayList<String> alreadyDoneTables = new ArrayList<String>();

		for (Rule rule : config.rules) {
			if (alreadyDoneTables.indexOf(rule.tableField.table) == -1) {
				constraintList.addAll(finder.findConstraintName(rule.tableField.table));
				alreadyDoneTables.add(rule.tableField.table);
			}
			for (TableField tableField : rule.dependants) {
				if (alreadyDoneTables.indexOf(tableField.table) == -1) {
					constraintList.addAll(finder.findConstraintName(tableField.table));
					alreadyDoneTables.add(tableField.table);
				}
			}
		}

		for (String tableName : scope.tables) {
			if (alreadyDoneTables.indexOf(tableName) == -1) {
				constraintList.addAll(finder.findConstraintName(tableName));
				alreadyDoneTables.add(tableName);
			}
		}

		return constraintList;
	}

	private void disableConstraints(ArrayList<Constraint> constraints, Connection connection) {
		anonymizerLogger.info("Disabling " + constraints.size() + " constraints.");
		for (Constraint constraint : constraints) {
			try (Statement disableStatement = connection.createStatement()) {
				disableStatement.execute(
						SQLHelper.disableForeignKey(connection, 
								constraint.schemaName + "." + constraint.tableName,
								constraint.constraintName));
			} catch (SQLException e) {
				anonymizerLogger.warning(String.format(
						"Could not disable constraint %s: %s",
						constraint.constraintName, e.getMessage()));
			}
		}
		anonymizerLogger.info("Finished: Disabling " + constraints.size() + " constraints.");
	}

	private void enableConstraints(ArrayList<Constraint> constraints, Connection connection) {
		anonymizerLogger.info("Enabling " + constraints.size() + " constraints.");
		for (Constraint constraint : constraints) {
			try (Statement disableStatement = connection.createStatement()) {
				disableStatement.execute(
						SQLHelper.enableForeignKey(connection, 
								constraint.schemaName + "." + constraint.tableName,
								constraint.constraintName));
			} catch (SQLException e) {
				anonymizerLogger.warning(String.format(
						"Could not enable constraint %s: %s",
						constraint.constraintName, e.getMessage()));
			}
		}
		anonymizerLogger.info("Finished: Enabling " + constraints.size() + " constraints.");
	}

	public List<Rule> getRulesFor(String tableName, String columnName)
			throws TableNotInScopeException {
		for (TableRuleMap tableRuleMap : tableRuleMaps)
			if (tableRuleMap.tableName.equalsIgnoreCase(tableName)) {
				return tableRuleMap.getRulesIgnoreCase(columnName);
			}
		throw new TableNotInScopeException();
	}
	
}
