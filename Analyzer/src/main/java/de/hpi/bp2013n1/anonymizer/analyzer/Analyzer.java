package de.hpi.bp2013n1.anonymizer.analyzer;

/*
 * #%L
 * Analyzer
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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.Anonymizer;
import de.hpi.bp2013n1.anonymizer.NoOperationStrategy;
import de.hpi.bp2013n1.anonymizer.RuleValidator;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class Analyzer {
	static final String NO_OP_STRATEGY_KEY = "";
	public Connection connection;
	public Config config;
	public Scope scope;
	
	public static class FatalError extends Exception {
		private static final long serialVersionUID = -2480888204710117127L;

		public FatalError(String message, Throwable cause,
				boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}

		public FatalError(String message, Throwable cause) {
			super(message, cause);
		}

		public FatalError(String message) {
			super(message);
		}

		public FatalError(Throwable cause) {
			super(cause);
		}
		
	}
	
	static Logger logger = Logger.getLogger(Analyzer.class.getName());
	private Map<String, TransformationStrategy> strategies;
	private RuleValidator ruleValidator;
	private Map<TableField, Rule> rulesByTableField;
	
	public Analyzer(Connection con, Config config, Scope scope){
		this.connection = con;
		this.config = config;
		this.scope = scope;
	}
	

	
	public void run(String outputFilename) throws FatalError {
		logger.fine("Starting analysis at " + Calendar.getInstance().getTime());
		setUpTransformationStrategies();
		// read meta tables and find dependents
		try {
			System.out.println("> Processing Rules");
			validateRulesAndAddDependants();
			
			System.out.println("> Validating");
			if (validateConfig()) {
				System.out.println("> Writing output file");
				// write to intermediary config file
				writeNewConfigToFile(outputFilename);
			}
			
			System.out.println("done");
			logger.fine("Finished writing config file at "
					+ Calendar.getInstance().getTime());
			
		} catch (SQLException e) {
			logger.severe("Database error: " + e.getMessage());
			throw new FatalError(e);
		} catch (IOException e) {
			logger.severe("Failed to write output file");
			throw new FatalError(e);
		}
	}


	void setUpTransformationStrategies() throws FatalError {
		strategies = loadAndInstanciateStrategies();
	}

	
	void validateRulesAndAddDependants() throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		addRulesForPrimaryKeyColumns(metaData);
		initializeRulesByTableField();
		findDependantsByForeignKeys(metaData);
		Iterator<Rule> ruleIterator = config.rules.iterator();
		ruleValidator = new RuleValidator(strategies, metaData);
		while (ruleIterator.hasNext()) {
			Rule rule = ruleIterator.next();
			System.out.println("  Processing: " + rule.tableField);
			if (!ruleValidator.isValid(rule)) {
				ruleIterator.remove();
				logger.warning("Skipping rule " + rule);
				continue;
			}
			validateExistingDependants(rule, metaData);
			findPossibleDependantsByName(rule, metaData);
		}
		removeNoOpRulesWithoutDependants();
	}



	void initializeRulesByTableField() {
		rulesByTableField = Maps.newHashMap();
		for (Rule rule : config.rules) {
			rulesByTableField.put(rule.tableField, rule);
		}
	}



	void addRulesForPrimaryKeyColumns(DatabaseMetaData metaData) throws SQLException {
		Multimap<String, Rule> rulesByParentTable = HashMultimap.create();
		for (Rule rule : config.rules) {
			rulesByParentTable.put(rule.tableField.table, rule);
		}
		try (ResultSet tablesResultSet = metaData.getTables(
				null, config.schemaName, null, new String[] { "TABLE" })) {
			while (tablesResultSet.next()) {
				String tableName = tablesResultSet.getString("TABLE_NAME");
				Collection<Rule> rulesForTable = rulesByParentTable.get(tableName);
				Multimap<String, Rule> rulesByColumn = HashMultimap.create();
				for (Rule rule : rulesForTable) {
					rulesByColumn.put(rule.tableField.column, rule);
				}
				try (ResultSet primaryKeyResultSet =
						metaData.getPrimaryKeys(null, config.schemaName, tableName)) {
					while (primaryKeyResultSet.next()) {
						String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
						if (!rulesByColumn.containsKey(columnName))
							addNoOpRule(config.schemaName, tableName, columnName);
					}
				}
			}
		}
	}


	private Rule addNoOpRule(String schemaName, String tableName,
			String columnName) {
		TableField tableField = new TableField(tableName, columnName, schemaName);
		return addNoOpRule(tableField);
	}


	private Rule addNoOpRule(TableField tableField) {
		Rule newRule = new Rule(
				tableField,
				NO_OP_STRATEGY_KEY, "");
		config.rules.add(newRule);
		return newRule;
	}


	boolean validateConfig() {
		// TODO: maybe we can safe one validate, or validate should be split
		config.validate();
		removeNoOpRulesWithoutDependants();
		return config.validate();
	}

	
	void findPossibleDependantsByName(Rule rule,
			DatabaseMetaData metaData) throws SQLException {
		try (ResultSet similarlyNamedColumns = metaData.getColumns(null, null,
				null, "%" + rule.tableField.column + "%")) {
			while (similarlyNamedColumns.next()) {
				if (!scope.tables.contains(
						similarlyNamedColumns.getString("TABLE_NAME")))
					continue;
				TableField newItem = new TableField(
						similarlyNamedColumns.getString("TABLE_NAME"),
						similarlyNamedColumns.getString("COLUMN_NAME"),
						config.schemaName);
				Rule ruleForSimilarlyNamedColumn = rulesByTableField.get(newItem);
				if (!(newItem.equals(rule.tableField)
						|| rule.dependants.contains(newItem)
						|| rule.potentialDependants.contains(newItem)
						|| (ruleForSimilarlyNamedColumn != null
								&& ruleForSimilarlyNamedColumn.dependants
									.contains(rule.tableField))))
					rule.potentialDependants.add(newItem);
			}
		}
	}

	
	void removeNoOpRulesWithoutDependants() {
		Iterator<Rule> ruleIterator = config.rules.iterator();
		while (ruleIterator.hasNext()) {
			Rule rule = ruleIterator.next();
			if (!rule.strategy.equals(NO_OP_STRATEGY_KEY))
				continue;
			if (rule.dependants.isEmpty() && rule.potentialDependants.isEmpty())
				ruleIterator.remove();
		}
	}


	void findDependantsByForeignKeys(DatabaseMetaData metaData) throws SQLException {
		// find all dependants by foreign keys
		for (String tableName : scope.tables) {
			try (ResultSet exportedKeys = metaData.getExportedKeys(
					null, null, tableName)) {
				addDependantsFromReferences(exportedKeys);
			}
			try (ResultSet importedKeys = metaData.getImportedKeys(
					null, null, tableName)) {
				addDependantsFromReferences(importedKeys);
			}
		}
	}


	private void addDependantsFromReferences(ResultSet referencesFromMetaData)
			throws SQLException {
		while (referencesFromMetaData.next()) {
			if (!scope.tables.contains(referencesFromMetaData.getString("FKTABLE_NAME")))
				continue;
			TableField pkTableField = new TableField(
					referencesFromMetaData.getString("PKTABLE_NAME"),
					referencesFromMetaData.getString("PKCOLUMN_NAME"),
					referencesFromMetaData.getString("PKTABLE_SCHEM"));
			if (!scope.tables.contains(pkTableField.table))
				continue;
			Rule ruleForPKColumn = rulesByTableField.get(pkTableField);
			if (ruleForPKColumn == null) {
				ruleForPKColumn = addNoOpRule(pkTableField);
				rulesByTableField.put(pkTableField, ruleForPKColumn);
			}
			TableField fkTableField = new TableField(
					referencesFromMetaData.getString("FKTABLE_NAME"),
					referencesFromMetaData.getString("FKCOLUMN_NAME"),
					referencesFromMetaData.getString("FKTABLE_SCHEM"));
			if (ruleForPKColumn.dependants.contains(fkTableField))
				continue;
			ruleForPKColumn.dependants.add(fkTableField);
		}
	}


	void validateExistingDependants(Rule rule, DatabaseMetaData metaData)
			throws SQLException {
		// verify predefined dependents
		Iterator<TableField> dependantIterator = rule.dependants.iterator();
		while (dependantIterator.hasNext()) {
			TableField dependant = dependantIterator.next();
			ResultSet columns = metaData.getColumns(null,
					dependant.schema,
					dependant.table,
					dependant.column);
			// there by definition is exactly one result set, or the field does not exist
			if (!columns.next()) {
				logger.severe("Dependant " + dependant + " does not exist in the schema. Skipping it.");
				dependantIterator.remove();
				continue;
			}
		}
	}


	private Map<String, TransformationStrategy> loadAndInstanciateStrategies()
			throws FatalError {
		// needed to create TransformationStrategy objects
		Anonymizer stubAnonymizer = new Anonymizer(config, scope);
		try {
			return loadAndInstanciateStrategies(stubAnonymizer);
		} catch (ClassNotFoundException | NoSuchMethodException
				| SecurityException | InstantiationException
				| IllegalAccessException | IllegalArgumentException e) {
			logger.severe("Could not load or instanciate strategy: "
				+ e.getMessage());
			throw new FatalError(e);
		} catch (InvocationTargetException e) {
			logger.severe("Could not load or instanciate strategy: "
					+ e.getCause().getMessage());
			throw new FatalError(e);
		}
	}



	private Map<String, TransformationStrategy> loadAndInstanciateStrategies(
			Anonymizer stubAnonymizer) throws ClassNotFoundException,
			NoSuchMethodException, InstantiationException,
			IllegalAccessException, InvocationTargetException {
		Map<String, TransformationStrategy> strategies = new HashMap<>();
		for (Map.Entry<String, String> strategyClassEntry : config.strategyMapping
				.entrySet()) {
			String strategyClassName = strategyClassEntry.getValue();
			strategies.put(strategyClassEntry.getKey(), TransformationStrategy
					.loadAndCreate(strategyClassName, stubAnonymizer,
							connection, null));
		}
		strategies.put(NO_OP_STRATEGY_KEY, TransformationStrategy.loadAndCreate(
				NoOperationStrategy.class.getName(),
				stubAnonymizer, connection, null));
		return strategies;
	}

	
	void writeNewConfigToFile(String outputFilename) throws IOException {
		File file = new File(outputFilename);
		try (FileWriter fw = new FileWriter(file);
				BufferedWriter writer = new BufferedWriter(fw)) {
			writeNewConfigTo(writer);
		}
	}


	// TODO: move to class Config
	private void writeNewConfigTo(Writer writer) throws IOException {
		writer.write("# originalDB newDB transformationDB each with username password\n");
		for (Config.ConnectionParameters parameters : new Config.ConnectionParameters[] {
				config.originalDB, config.destinationDB, config.transformationDB
		}) {
			writer.write(parameters.url);
			writer.write(" ");
			writer.write(parameters.user);
			writer.write(" ");
			writer.write(parameters.password);
			writer.write("\n");
		}
		writer.write("# schema name and batch size\n");
		writer.write(config.schemaName);
		writer.write(" ");
		writer.write(Integer.toString(config.batchSize));
		writer.write("\n\n");
		for (Map.Entry<String, String> pair : config.strategyMapping.entrySet()) {
			writer.write(String.format("- %s: %s\n", pair.getKey(), pair.getValue()));
		}
		writer.write("\n# Table.Field\t\tType\t\tAdditionalInfo\n");
		for (Rule rule : config.rules) {
			if (rule.dependants.isEmpty() && rule.strategy.equals(NO_OP_STRATEGY_KEY))
				writer.write('#');
			writer.write(rule.tableField.toString());
			if (!rule.strategy.equals(NO_OP_STRATEGY_KEY)) {
				writer.write("\t");
				writer.write(rule.strategy);
			}
			
			if (!rule.additionalInfo.isEmpty()) {
				writer.write("\t");
				writer.write(rule.additionalInfo);
				writer.write("\n");
			} else
				writer.write("\n");
			
			for (TableField dependent : rule.dependants) {
				writer.write("\t" + dependent + "\n");
			}
			
			for (TableField dependent : rule.potentialDependants) {
				writer.write("\t#" + dependent + "\n");
			}
		}
	}
}
