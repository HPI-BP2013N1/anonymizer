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


import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.ColumnTypeNotSupportedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.FetchPseudonymsFailedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.PreparationFailedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.TransformationFailedException;
import de.hpi.bp2013n1.anonymizer.db.BatchOperation;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.InvalidConfigurationException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;
import de.hpi.bp2013n1.anonymizer.tools.ConstraintToggler;
import de.hpi.bp2013n1.anonymizer.tools.TableTruncater;
import de.hpi.bp2013n1.anonymizer.util.RuleConnector;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

/**
 * Application class that conducts an anonymization run on databases.
 */
public class Anonymizer {
	
	public Config config;
	public Scope scope;
	private Connection originalDatabase, anonymizedDatabase, transformationDB;
	private ArrayList<TransformationStrategy> transformationStrategies = new ArrayList<>();
	private TreeMap<String, TransformationStrategy> strategyByClassName = new TreeMap<>();
	private Map<String, TransformationStrategy> strategyByName = new TreeMap<>();
	private final int LOG_INTERVAL = 1000;
	
	public static final Logger anonymizerLogger = Logger.getLogger(Anonymizer.class.getName());
	private static FileHandler logFileHandler;
	private static SimpleFormatter logFormatter;
	ConstraintToggler toggler = new ConstraintToggler();
	private RowRetainService retainService;
	private ForeignKeyDeletionsHandler foreignKeyDeletions = new ForeignKeyDeletionsHandler();
	private boolean skipRuleValidation;
	Multimap<TableField, Rule> comprehensiveRulesBySite;
	
	public static class TableNotInScopeException extends Exception {
		private static final long serialVersionUID = -4527921975005958468L;
	}
	
	public static class FatalError extends Exception {
		private static final long serialVersionUID = -6431519862013506163L;

		public FatalError() {
			super();
		}

		public FatalError(Throwable cause) {
			super(cause);
		}
	}
	
	public static class TableNotFoundException extends Exception {
		private static final long serialVersionUID = -852972263392782109L;

		public TableNotFoundException(String message) {
			super(message);
		}
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

	/**
	 * Initializes transformation strategies, valides the configuration and
	 * then performs the actual anonymization. Catches all declared exceptions
	 * and wraps them as FatalError.
	 */
	public void run() throws FatalError {
		try {
			loadAndInstantiateStrategies();
		} catch (InvalidConfigurationException e) {
			throw new FatalError(e);
		} catch (ClassNotFoundException e) {
			anonymizerLogger.severe("Could not load strategy: " + e.getMessage());
			throw new FatalError(e);
		}
		// getConstructor
		catch (NoSuchMethodException e) {
			anonymizerLogger.severe("Strategy is missing the required constructor: "
					+ e.getMessage());
			throw new FatalError(e);
		} catch (SecurityException e) {
			anonymizerLogger.severe("Could not access strategy constructor: "
					+ e.getMessage());
			throw new FatalError(e);
		}
		// newInstance
		catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			anonymizerLogger.severe("Could not create strategy: " + e.getMessage());
			throw new FatalError(e);
		}
		// not a TransformationStrategy
		catch (ClassCastException e) {
			// error message has already been emitted in loadAndInstanciateStrategy
			throw new FatalError(e);
		}
	
		try {
			if (skipRuleValidation || validateRules() == 0)
				try {
					anonymize();
				} catch (TransformationTableCreationException e) {
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				} catch (TransformationKeyCreationException e) {
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				} catch (FetchPseudonymsFailedException e) {
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				} catch (ColumnTypeNotSupportedException e) {
					anonymizerLogger.severe("An anonymization strategy does not "
							+ "support the type of column to which the strategy "
							+ "should be applied: " + e.getMessage());
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				} catch (PreparationFailedException e) {
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				} catch (TableNotFoundException e) {
					logSevereErrorAndCausesWithInnerStackTrace(e);
					throw new FatalError(e);
				}
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while checking whether all "
					+ "values will fit into their column: "
					+ e.getMessage());
			throw new FatalError(e);
		}
	}

	private void logSevereErrorAndCausesWithInnerStackTrace(Throwable t) {
		Throwable last = t;
		anonymizerLogger.severe(t.getMessage());
		while (t != null) {
			anonymizerLogger.severe("caused by: " + t.getMessage());
			last = t;
			t = t.getCause();
		}
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		last.printStackTrace(pw);
		anonymizerLogger.severe("stack trace of inner exception: "
				+ sw.toString());
		if (last instanceof SQLException) {
			SQLException e = (SQLException) last;
			e = e.getNextException();
			anonymizerLogger.severe("SQLException detected, exception chain follows");
			while (e != null) {
				anonymizerLogger.severe(e.getMessage());
				e = e.getNextException();
			}
		}
	}
	
	public void loadAndInstantiateStrategies() throws ClassNotFoundException,
			NoSuchMethodException, InstantiationException,
			IllegalAccessException, InvocationTargetException,
			ClassCastException, InvalidConfigurationException {
		anonymizerLogger.info("Loading transformation strategies.");
		for (Map.Entry<String, String> strategiesEntry : config.getStrategyMapping().entrySet()) {
			String strategy = strategiesEntry.getKey();
			String strategyClassName = strategiesEntry.getValue();
			checkNotNull(strategyClassName, "No strategy class defined for " + strategy);
			if (!strategyByClassName.containsKey(strategyClassName)) {
				loadAndInstanciateStrategy(strategyClassName);
			}
			TransformationStrategy transformationStrategy =
					strategyByClassName.get(strategyClassName);
			strategyByName.put(strategy, transformationStrategy);
		}
		for (Rule rule : config.rules) {
			String strategy = rule.getStrategy();
			if (!strategyByName.containsKey(strategy))
				throw new Config.InvalidConfigurationException("Strategy "
						+ strategy + " is not defined but used in rule " + rule);
			rule.setTransformation(strategyByName.get(strategy));
		}
	}
	
	protected void loadAndInstanciateStrategy(String strategyClassName)
					throws ClassNotFoundException, NoSuchMethodException,
					InstantiationException, IllegalAccessException,
					InvocationTargetException, ClassCastException {
		System.out.println("Loading " + strategyClassName);
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

		} catch (SQLException e) {
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
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Expected 3 Arguments\n" +
					"1. : path to intermediary config file, \n" +
					"2. : path to scope file,\n" +
					"3. : desired name of logfile");
			System.exit(64);
			return;
		}
		
		List<String> arguments = Lists.newArrayList(args);
		boolean skipRuleValidation = arguments.remove("--skip-rule-validation");
			
		try {
			setUpLogging(arguments.get(2));
		} catch (IOException e) {
			anonymizerLogger.severe("Could not set up logging to the specified "
					+ "file: " + e.getMessage());
			System.exit(74);
		}
				
		Config config = new Config();
		Scope scope = new Scope();
		try {
			anonymizerLogger.info("Reading config file.");
			config.readFromFile(arguments.get(0));
		} catch (IOException e) {
			anonymizerLogger.severe("Could not read from config file: "
					+ e.getMessage());
			System.exit(74);
			return;
		} catch (DependantWithoutRuleException | MalformedException e) {
			anonymizerLogger.severe("Invalid config file: " + e.getMessage());
			System.exit(78);
			return;
		}
		try {
			anonymizerLogger.info("Reading scope file");
			scope.readFromFile(arguments.get(1));
		} catch (IOException e) {
			anonymizerLogger.severe("Reading scope file failed: "
					+ e.getMessage());
			System.exit(74);
			return;
		}
		Anonymizer anon = new Anonymizer(config, scope);
		if (skipRuleValidation)
			anon.skipRuleValidation = true;
		try {
			anon.connectAndRun();
		} catch (FatalError e) {
			anonymizerLogger.severe("Cannot recover from previous errors, exiting.");
			System.exit(1);
		}
	}

	/**
	 * Overrides the logging format and also routes logging output to the
	 * specified log file.
	 * 
	 * @param logFilename path to the log file to which logging output should be written
	 * @throws IOException cannot initialize FileHandler for the logfile
	 */
	public static void setUpLogging(String logFilename) throws IOException {
		System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] - %4$s: %5$s (%2$s)%n");
		Logger logger = Logger.getLogger("de.hpi.bp2013n1");
		logFileHandler = new FileHandler(logFilename);
		
		logFormatter = new SimpleFormatter();
						
		logger.addHandler(logFileHandler);
		logFileHandler.setFormatter(logFormatter);
	}
	
	public int validateRules() throws SQLException {
		anonymizerLogger.info("Checking whether the transformation rules are valid.");
		int numberOfErrors = 0;
		RuleValidator ruleValidator = new RuleValidator(anonymizedDatabase.getMetaData());
		for (Rule rule : config.rules) {
			if (!ruleValidator.isValid(rule)) {
				numberOfErrors++;
			}
		}
		return numberOfErrors;
	}

	/**
	 * Main anonymization method. Creates transformation tables in
	 * transformation database, then copies and/or anonymizes the data.
	 * During the transformation and copying, every relevant constraint in the
	 * destination database will be disabled.
	 * 
	 * @throws ColumnTypeNotSupportedException
	 * @throws TransformationTableCreationException
	 * @throws TransformationKeyCreationException
	 * @throws FetchPseudonymsFailedException
	 * @throws PreparationFailedException
	 */
	public void anonymize() throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedException,
			TableNotFoundException {
		anonymizerLogger.info("Started anonymizing.");
		checkIfTablesExistInDestinationDatabase();
		try {
			foreignKeyDeletions.determineForeignKeysAmongTables(originalDatabase,
					config.schemaName, scope.tables);
		} catch (SQLException e) {
			anonymizerLogger.severe("Could not determine relationships in the "
					+ "source database: " + e.getMessage());
		}
		foreignKeyDeletions.addForeignKeysForRuleDependents(config.rules);
		Collection<Constraint> constraints = disableAnonymizedDbConstraints();

		prepareTransformations();
		copyAndAnonymizeData();

		ConstraintToggler.enableConstraints(constraints, anonymizedDatabase);
		anonymizerLogger.info("Finished: Anonymizing");
		for (TransformationStrategy strategy : transformationStrategies)
			strategy.printSummary();
	}

	private void checkIfTablesExistInDestinationDatabase()
			throws TableNotFoundException {
		try {
			DatabaseMetaData metaData = anonymizedDatabase.getMetaData();
			String[] tableTypes = new String[] { "TABLE" };
			List<String> missingTables = new ArrayList<>();
			for (String tableName : scope.tables) {
				try (ResultSet tables = metaData.getTables(
						null, config.schemaName, tableName, tableTypes)) {
					if (!tables.next())
						missingTables.add(SQLHelper.qualifiedTableName(
								config.schemaName, tableName));
				}
			}
			if (!missingTables.isEmpty())
				throw new TableNotFoundException(
						"The following tables could not be found:\n"
								+ Joiner.on('\n').join(missingTables));
		} catch (SQLException e) {
			anonymizerLogger.severe("Could not determine if all tables exist "
					+ "in the destination database. This may lead to a lot of "
					+ "error messages if some tables are really missing. "
					+ "The error was: " + e.getMessage());
		}
	}

	private Collection<Constraint> disableAnonymizedDbConstraints() {
		return ConstraintToggler.disableConstraints(anonymizedDatabase, config, scope);
	}
	
	private void copyAndAnonymizeData() {
		anonymizerLogger.info("Started copying data.");
		try {
			anonymizedDatabase.setAutoCommit(false);
		} catch (SQLException | AbstractMethodError e) {
			// no performance gain but not severe
		}
		
		collectRulesBySite();
		int currentTableNumber = 0;
		for (String table : scope.tables) {
			TableRuleMap ruleMap = buildTableRuleMapFor(table);
			anonymizerLogger.info("Copying data from: " + table +
					" (table " + (++currentTableNumber)
					+ "/" + scope.tables.size() + ").");
			copyAndAnonymizeTable(ruleMap);
		}
		
		try {
			anonymizedDatabase.setAutoCommit(true);
		} catch (SQLException | AbstractMethodError e) {
			// probably disabling it earlier failed as well
		}
		anonymizerLogger.info("Finished: Copying Data.");
	}

	private TableRuleMap buildTableRuleMapFor(String table) {
		TableField tableSite = new TableField(table, null, config.schemaName);
		TableRuleMap ruleMap = new TableRuleMap(table);
		for (Map.Entry<TableField, Rule> fieldAndRule : comprehensiveRulesBySite.entries()) {
			TableField tableField = fieldAndRule.getKey();
			if (tableField.asTableSite().equals(tableSite)) {
				Rule rule = fieldAndRule.getValue();
				ruleMap.put(tableField.getColumn(), rule);
			}
		}
		return ruleMap;
	}

	/**
	 * Creates a multimap which maps applied strategies to a TableField
	 * (table or attribtue) in the correct order of application.
	 */
	void collectRulesBySite() {
		RuleConnector ruleConnector = new RuleConnector();
		ruleConnector.addRules(config.getRules());
		Multimap<TableField, Rule> rulesBySite = ruleConnector.getRulesBySite();
		comprehensiveRulesBySite = LinkedHashMultimap.create();
		for (Map.Entry<TableField, Rule> siteAndRule : rulesBySite.entries()) {
			TableField thisSite = siteAndRule.getKey();
			ArrayList<Rule> rulesToApplyToThisSite = new ArrayList<>();
			Rule rule = siteAndRule.getValue();
			rulesToApplyToThisSite.addAll(rule.transitiveParents());
			rulesToApplyToThisSite.add(rule);
			comprehensiveRulesBySite.putAll(thisSite, rulesToApplyToThisSite);
		}
		
		Iterator<Rule> rulesIterator = comprehensiveRulesBySite.values().iterator();
		while (rulesIterator.hasNext()) {
			Rule eachRule = rulesIterator.next();
			if (eachRule.getTransformation() == null) {
				eachRule.setTransformation(strategyByName.get(eachRule.getStrategy()));
			}
			if (eachRule.getTransformation() instanceof NoOperationStrategy) {
				rulesIterator.remove();
			}
		}
	}

	private void copyAndAnonymizeTable(TableRuleMap tableRuleMap) {
		// make sure target newDB is empty
		String qualifiedTableName = config.schemaName + "." + tableRuleMap.tableName;
		truncateTable(qualifiedTableName);
		ResultSetMetaData rsMeta;
		int rowCount = countRowsInTable(qualifiedTableName);
		if (rowCount > 0)
			anonymizerLogger.info("Found " + rowCount + " rows.");
		try (PreparedStatement selectStarStatement = originalDatabase.prepareStatement(
				"SELECT * FROM " + qualifiedTableName);
				ResultSet rs = selectStarStatement.executeQuery()) {
			try {
				rsMeta = rs.getMetaData();
				
				for (TransformationStrategy strategy : transformationStrategies) {
					TableRuleMap tableRuleMapForStrategy =
							tableRuleMap.filteredByStrategy(strategy);
					if (tableRuleMapForStrategy.isEmpty())
						continue;
					strategy.prepareTableTransformation(tableRuleMapForStrategy);
				}
				
			} catch (SQLException | FetchPseudonymsFailedException e) {
				anonymizerLogger.warning("Fetching rows failed: " + e.getMessage());
				e.printStackTrace();
				return;
			}

			copyAndAnonymizeRows(tableRuleMap, qualifiedTableName, rsMeta,
					rowCount, rs);

			try {
				anonymizedDatabase.commit();
			} catch (SQLException e) {
				anonymizerLogger.warning("Commit operation concluding table "
						+ qualifiedTableName + " failed.");
			}
		} catch (SQLException e) {
			anonymizerLogger.severe("Could not query table "
					+ qualifiedTableName + ": " + e.getMessage());
		}
	}

	private void copyAndAnonymizeRows(TableRuleMap tableRuleMap,
			String qualifiedTableName, ResultSetMetaData rsMeta,
			int rowCount, ResultSet rs) throws SQLException {
		// prepared Statement for batch loading
		int columnCount = rsMeta.getColumnCount();
		List<String> columnNames = new ArrayList<>();
		for (int column = 1; column <= columnCount; column++) {
			columnNames.add(rsMeta.getColumnName(column));
		}
		StringBuilder insertQueryBuilder = new StringBuilder();
		insertQueryBuilder.append("INSERT INTO ")
		.append(qualifiedTableName)
		.append(" (")
		.append(Joiner.on(',').join(columnNames))
		.append(") VALUES (");
		for (int j = 0; j < columnCount - 1; j++)
			insertQueryBuilder.append("?,");
		insertQueryBuilder.append("?)");
		
		ResultSetRowReader rowReader = new ResultSetRowReader(rs);
		rowReader.setCurrentTable(tableRuleMap.tableName);
		rowReader.setCurrentSchema(config.schemaName);
		try (PreparedStatement insertStatement = anonymizedDatabase.prepareStatement(
				insertQueryBuilder.toString())) {
			int processedRowsCount = 0;
			while (!rs.isClosed() && rs.next()) { // for all rows
				try {
					copyAndAnonymizeRow(tableRuleMap, qualifiedTableName, rsMeta,
							columnCount, rowReader, insertStatement);
				} catch (SQLException e) {
					anonymizerLogger.severe("SQL error when transforming row #"
							+ (processedRowsCount + 1) + ": " + e.getMessage());
				}
				processedRowsCount++;
				if ((processedRowsCount % config.batchSize) == 0) {
					try {
						BatchOperation.executeAndCommit(insertStatement);
					} catch (SQLException e) {
						logBatchInsertError(e);
					}
				}
				
				if ((processedRowsCount % LOG_INTERVAL) == 0)
					System.out.format("Progress: %d/%d (%d %%)\r", processedRowsCount, rowCount, 100 * processedRowsCount / rowCount);
				
			}
			if ((processedRowsCount % LOG_INTERVAL) != 0)
				System.out.format("Progress: %d/%d (%d %%)\n", processedRowsCount, rowCount, 100 * processedRowsCount / rowCount);
			else
				System.out.format("\n");
			
			try {
				BatchOperation.executeAndCommit(insertStatement);
			} catch (SQLException e) {
				logBatchInsertError(e);
			}
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while traversing table "
					+ qualifiedTableName + ": " + e.getMessage());
		}
	}

	private void logBatchInsertError(SQLException e) {
		anonymizerLogger.severe("Error(s) during batch insert: " + e.getMessage());
		for (Throwable chainedException : Iterables.skip(e, 1)) {
			anonymizerLogger.severe("Insert error: "
					+ chainedException.getMessage());
		}
	}

	private void copyAndAnonymizeRow(TableRuleMap tableRuleMap,
			String qualifiedTableName, ResultSetMetaData rsMeta,
			int columnCount, ResultSetRowReader rowReader,
			PreparedStatement insertStatement) throws SQLException {
		boolean retainRow = false;
		if (foreignKeyDeletions.hasParentRowBeenDeleted(rowReader)) {
			retainRow = retainService.currentRowShouldBeRetained(
					config.schemaName, tableRuleMap.tableName, rowReader);
			if (retainRow) {
				anonymizerLogger.warning("Retaining a row in "
						+ qualifiedTableName + " even though its parent row "
								+ "in another table has been deleted!");
			} else {
				// the parent row has been deleted, so delete this row as well
				// since the foreign key cannot be reestablished and should
				// possible be kept secret
				foreignKeyDeletions.rowHasBeenDeleted(rowReader);
				return;
			}
		}
		// apply rules which have no column name specified first
		// because these are likely to be retain or delete instructions
		for (Rule configRule : tableRuleMap.getRules(null)) {
			if (Iterables.isEmpty(
					anonymizeValue(null, configRule, rowReader, null, tableRuleMap))) {
				if (retainRow
						|| retainService.currentRowShouldBeRetained(
								configRule.getTableField().schema,
								configRule.getTableField().table,
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
				foreignKeyDeletions.rowHasBeenDeleted(rowReader);
				return;
			}
		}
		// apply rules to specific columns
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
								intermediateValue, configRule, rowReader, columnName,
								tableRuleMap);
						newValues = Iterables.concat(newValues,
								transformationResults);
					}
					if (!newValues.iterator().hasNext()) {
						if (retainRow
								|| retainService.currentRowShouldBeRetained(
										configRule.getTableField().schema,
										configRule.getTableField().table,
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
						foreignKeyDeletions.rowHasBeenDeleted(rowReader);
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
			addBatchInserts(insertStatement, columnValues);
		} catch (SQLException e) {
			anonymizerLogger.severe("Adding insert statement failed: "
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
			Rule configRule, ResultSetRowReader rowReader, String columnName,
			TableRuleMap tableRules) {
		TransformationStrategy strategy;
		strategy = configRule.getTransformation();
		try {
			return strategy.transform(currentValue, configRule, rowReader);
		} catch (TransformationKeyNotFoundException e) {
			anonymizerLogger.log(Level.SEVERE,
					"Transformation value for \"" +
							currentValue + "\" (from table " +
					tableRules.tableName + "." + columnName +
					") was not found in keys for " +
					configRule.getTableField() +
					". Used empty String instead.",
					e);
		} catch (SQLException e) {
			anonymizerLogger.severe("SQL error while transforming value \""
					+ currentValue + "\" (from table " +
					tableRules.tableName + "." + columnName +
					") : " + e.getMessage() + ". Using empty String instead.");
		} catch (TransformationFailedException e) {
			anonymizerLogger.severe(e.getMessage());
			Throwable t = e;
			while ((t = t.getCause()) != null) {
				anonymizerLogger.severe("caused by: " + t.getMessage());
			}
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
		TableTruncater.truncateTable(qualifiedTableName, anonymizedDatabase);
	}

	private void prepareTransformations() throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedException {
		anonymizerLogger.info("Setting up transformations.");
		try {
			createSchemaInTransformataionDatabase();
		} catch (SQLException e) {
			anonymizerLogger.warning("Could not create schema in the "
					+ "transformation database, this might cause the creation "
					+ "of pseudonym tables or others to fail.");
		}
		Multimap<TransformationStrategy, Rule> rulesByStrategy = ArrayListMultimap.create();
		for (Rule rule : config.rules) {
			if (!scope.tables.contains(rule.getTableField().table)) {
				anonymizerLogger.warning("Table " + rule.getTableField().table
						+ " not in scope. Skipping dependants and continuing.");
				continue;
			}
			rulesByStrategy.put(rule.getTransformation(), rule);
            for (TableField dependant : rule.getDependants()) {
				if (!scope.tables.contains(dependant.table)){
					anonymizerLogger.warning("Dependend table " + dependant.table
							+ " not in scope. Skipping dependant and continuing.");
					continue;
				}
			}
		}
		for (TransformationStrategy strategy : rulesByStrategy.keySet()) {
			anonymizerLogger.info("Setting up transformations for " + strategy.getClass().getSimpleName());
			strategy.setUpTransformation(rulesByStrategy.get(strategy));
		}
		anonymizerLogger.info("Finished: setting up transformations.");
	}

	private void createSchemaInTransformataionDatabase() throws SQLException {
		try (ResultSet schemasResult = transformationDB.getMetaData()
				.getSchemas(null, config.schemaName)) {
			if (schemasResult.next())
				return; // schema is already present
		}
		SQLHelper.createSchema(config.schemaName, transformationDB);
	}

	/**
	 * Retrieves all rules which are applied to the specified attribute in the
	 * order of application.
	 * 
	 * @param tableName table which contains the attribute
	 * @param columnName column name of the attribute
	 */
	public Collection<Rule> getRulesFor(String tableName, String columnName) {
		return comprehensiveRulesBySite.get(
				new TableField(tableName, columnName, config.getSchemaName()));
	}
	
	public boolean isTableInScope(TableField tableSite) {
		return scope.tables.contains(tableSite.getTable());
	}
	
	public boolean isTableInScope(String tableName) {
		return scope.tables.contains(tableName);
	}
	
}
