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
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.Anonymizer;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class Analyzer {
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
	
	public Analyzer(Connection con, Config config, Scope scope){
		this.connection = con;
		this.config = config;
		this.scope = scope;
	}
	

	
	public void run(String outputFilename) throws FatalError {
		logger.fine("Starting analysis at " + Calendar.getInstance().getTime());
		strategies = loadAndInstanciateStrategies();
		// read meta tables and find dependents
		try {
			System.out.println("> Processing Rules");
			Iterator<Rule> ruleIterator = config.rules.iterator();
			DatabaseMetaData metaData = connection.getMetaData();
			while (ruleIterator.hasNext()) {
				Rule rule = ruleIterator.next();
				System.out.println("  Rule: " + rule.tableField);
				if (!validateRule(rule, metaData)) {
					ruleIterator.remove();
					logger.warning("Skipping rule " + rule);
					continue;
				}
				validateExistingDependants(rule, metaData);
				findDependantsByForeignKeys(rule, metaData);
				findPossibleDependantsByName(rule, metaData);
			}
			
			System.out.println("> Validating");
			if (config.validate()) {
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
				if (!(newItem.equals(rule.tableField) 
						|| rule.dependants.contains(newItem)
						|| rule.potentialDependants.contains(newItem)))
					rule.potentialDependants.add(newItem);
			}
		}
	}



	void findDependantsByForeignKeys(Rule rule,
			DatabaseMetaData metaData) throws SQLException {
		Queue<TableField> checkFields = new ArrayDeque<TableField>();
		checkFields.add(rule.tableField);
		// find all dependants by foreign keys
		while (!checkFields.isEmpty()) {
			TableField currentField = checkFields.remove();
			ResultSet exportedKeys = metaData.getExportedKeys(
					null, null, currentField.table);
			while (exportedKeys.next()) {
				if (!scope.tables.contains(exportedKeys.getString("FKTABLE_NAME")))
					continue;
				if (exportedKeys.getString("PKCOLUMN_NAME")
						.equals(currentField.column)) {
					TableField newitem = new TableField(
							exportedKeys.getString("FKTABLE_NAME"),
							exportedKeys.getString("FKCOLUMN_NAME"), 
							config.schemaName);
					if (rule.dependants.contains(newitem))
						continue;
					rule.dependants.add(newitem);
					checkFields.add(newitem);
				}
			}
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



	private boolean validateRule(Rule rule, DatabaseMetaData metaData) {
		String typename = "";
		int length = 0;
		boolean nullAllowed = false;
		if (rule.tableField.column != null) {
			try (ResultSet column = metaData.getColumns(null, 
					rule.tableField.schema, 
					rule.tableField.table,
					rule.tableField.column)) {
				column.next();
				typename = column.getString("TYPE_NAME");
				length = column.getInt("COLUMN_SIZE");
				nullAllowed = column.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
			} catch (SQLException e) {
				logger.severe("Field " + rule.tableField + " does not exist in the schema.");
				return false;
			}
		}
		
		// let the strategies validate their rules
		boolean strategyIsValid = true;
		try {
			strategyIsValid = strategies.get(rule.strategy).isRuleValid(
					rule, typename, length, nullAllowed);
		} catch (RuleValidationException e) {
			logger.severe("Could not validate rule " + rule + ": " 
					+ e.getMessage());
			return false;
		}
		if (!strategyIsValid) {
			return false;
		}
		return true;
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
		return strategies;
	}

	
	private void writeNewConfigToFile(String outputFilename) throws IOException {
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
			writer.write(rule.tableField + "\t");
			writer.write(rule.strategy);
			
			if (rule.additionalInfo.length() > 0)
				writer.write("\t" + rule.additionalInfo + "\n");
			else
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
