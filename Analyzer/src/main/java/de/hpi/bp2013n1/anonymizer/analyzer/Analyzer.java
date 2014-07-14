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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
	
	static Logger logger = Logger.getLogger(Analyzer.class.getName());
	
	public Analyzer(Connection con, Config config, Scope scope){
		this.connection = con;
		this.config = config;
		this.scope = scope;
	}
	

	
	public void run(String outputFilename){
		 // needed to create TransformationStrategy objects
		Anonymizer stubAnonymizer = new Anonymizer(config, scope);
		// load and create TransformationStrategies
		Map<String, TransformationStrategy> strategies;
		try {
			strategies = loadAndInstanciateStrategies(stubAnonymizer);
		} catch (ClassNotFoundException | NoSuchMethodException
				| SecurityException | InstantiationException
				| IllegalAccessException | IllegalArgumentException e) {
			logger.severe("Could not load or instanciate strategy: " 
				+ e.getMessage());
			return;
		} catch (InvocationTargetException e) {
			logger.severe("Could not load or instanciate strategy: " 
					+ e.getCause().getMessage());
			return;
		}
		// read meta tables and find dependents
		try {
			System.out.println("> Processing Rules");
			int i = 0;
			DatabaseMetaData metaData = connection.getMetaData();
			while(i < config.rules.size()){
				Rule rule = config.rules.get(i);
				System.out.println("  Rule: " + rule.tableField);
				
				ArrayList<TableField> checkFields = new ArrayList<TableField>();
				checkFields.add(rule.tableField);
				
				// verify parameters are valid
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
					} catch(Exception e) {
						logger.severe("This field does not exist in the schema.");
						config.rules.remove(i);
						logger.warning("Skipping rule " + rule);
						continue;
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
					config.rules.remove(i);
					logger.warning("Skipping rule " + rule);
					continue;
				}
				if (!strategyIsValid) {
					config.rules.remove(i);
					logger.warning("Skipping rule " + rule);
					continue;
				}

				// verify predefined dependents
				for (int j = 0; j < rule.dependants.size(); j++) {
					ResultSet columns = metaData.getColumns(null, 
							rule.dependants.get(j).schema,
							rule.dependants.get(j).table,
							rule.dependants.get(j).column);
					// there by definition is exactly one result set, or the field does not exist
					if (!columns.next()) {
						System.out.println("ERROR: Dependant " + rule.dependants.get(j) + " does not exist in the schema. Skipping");
						rule.dependants.remove(j);
						continue;
					}
					j++;
				}				
				
				// find all dependents
				while (checkFields.size() > 0) {
					TableField currentField = checkFields.get(0);
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
							rule.dependants.add(newitem);
							checkFields.add(newitem);
						}
					}
					checkFields.remove(0);
				}

				// search for potential additional dependents
				ResultSet similarlyNamedColumns = metaData.getColumns(null, null, 
						null, "%" + rule.tableField.column + "%");
				while (similarlyNamedColumns.next()) {
					if (!scope.tables.contains(
							similarlyNamedColumns.getString("TABLE_NAME")))
						continue;
					TableField newItem = new TableField(
							similarlyNamedColumns.getString("TABLE_NAME"),
							similarlyNamedColumns.getString("COLUMN_NAME"), 
							config.schemaName);
					if (!(newItem.equals(rule.tableField) 
							|| rule.dependants.contains(newItem)))
						rule.potentialDependants.add(newItem);
				}
				
				
				i++;
			}
			
			System.out.println("> Validating");
			boolean valid = config.validate();
			
			if (valid) {
				System.out.println("> Writing output file");
				// write to intermediary config file
				writeNewConfigToFile(outputFilename);
			}
			
			System.out.println("done");
			
		} catch (SQLException e) {
			System.err.println("Database error: " + e.getMessage());
		} catch (IOException e) {
			System.err.println("Failed to write output file");
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
