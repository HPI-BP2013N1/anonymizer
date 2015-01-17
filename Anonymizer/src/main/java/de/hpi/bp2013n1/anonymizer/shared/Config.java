package de.hpi.bp2013n1.anonymizer.shared;

/*
 * #%L
 * AnonymizerShared
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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.bp2013n1.anonymizer.NoOperationStrategy;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy;
import de.hpi.bp2013n1.anonymizer.db.TableField;

public class Config {
	private static final Charset CONFIG_CHARSET = StandardCharsets.UTF_8;
	public static final String NO_OP_STRATEGY_KEY = "";
	
	public static class MalformedException extends Exception {
		private static final long serialVersionUID = 4579175683815643144L;

		public MalformedException() {
			super();
		}

		public MalformedException(String message, Throwable cause) {
			super(message, cause);
		}

		public MalformedException(String message) {
			super(message);
		}
	}
	
	public static class InvalidConfigurationException extends Exception {
		private static final long serialVersionUID = 4125870021463484369L;

		public InvalidConfigurationException(String message) {
			super(message);
		}
	}

	public static class DependantWithoutRuleException extends Exception {
		public DependantWithoutRuleException() {
			super();
		}

		public DependantWithoutRuleException(String message) {
			super(message);
		}

		private static final long serialVersionUID = 1307243018122412272L;
	}
	public class ConnectionParameters {
		public String url;
		public String user;
		public String password;
	}
	public List<Rule> rules = new ArrayList<Rule>();
	public ConnectionParameters originalDB = new ConnectionParameters();
	public ConnectionParameters destinationDB = new ConnectionParameters();
	public ConnectionParameters transformationDB = new ConnectionParameters();
	public String schemaName;
	public int batchSize;
	public Map<String, String> strategyMapping = new HashMap<>();
	
	private static Logger configLogger = Logger.getLogger(Config.class.getName());
	
	public Config() {
		strategyMapping.put(NO_OP_STRATEGY_KEY, NoOperationStrategy.class.getName());
	}
	
	public List<Rule> getRules() {
		return rules;
	}
	
	public boolean addRule(Rule newRule) {
		return rules.add(newRule);
	}
	
	public boolean removeRule(Rule aRule) {
		return rules.remove(aRule);
	}

	public ConnectionParameters getOriginalDB() {
		return originalDB;
	}

	public ConnectionParameters getDestinationDB() {
		return destinationDB;
	}

	public ConnectionParameters getTransformationDB() {
		return transformationDB;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public Map<String, String> getStrategyMapping() {
		return strategyMapping;
	}

	public static Config fromFile(String fileName) throws DependantWithoutRuleException, IOException, MalformedException {
		Config config = new Config();
		config.readFromFile(fileName);
		return config;
	}
	
	public void readFromFile(String filename) throws DependantWithoutRuleException, IOException, MalformedException {
		try (BufferedReader reader = Files.newBufferedReader(
				FileSystems.getDefault().getPath(filename), CONFIG_CHARSET)) {
			read(reader);
		}
	}
	
	public void readFromURL(URL url) throws DependantWithoutRuleException, IOException, MalformedException {
		try (InputStream inputStream = url.openStream();
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
			read(bufferedReader);
		}
	}
	
	public void read(BufferedReader reader)
			throws DependantWithoutRuleException, IOException, MalformedException {
		String line;
		readHeader(reader);
		Pattern keyValuePattern = Pattern.compile(
				"\\s*-\\s*(\\w+)\\s*:\\s*([\\w.]+)");
		while (true) {
			line = reader.readLine();
			if(line == null) break;
			if(line.matches("\\s*#.*")) continue;
			if(line.matches("\\s*")) continue;
			
			if (line.trim().startsWith("-")) {
				Matcher matcher = keyValuePattern.matcher(line);
				if (!matcher.matches())
					continue;
				String strategyName = matcher.group(1);
				String strategyClassName = matcher.group(2);
				strategyMapping.put(strategyName, strategyClassName);
			} else if (line.matches("\\s+.*")) { // it's a dependant
				readDependant(line);
			} else { // it's a new rule
				readNewRule(line);
			}
		}
	}

	void readDependant(String line) throws DependantWithoutRuleException {
		if (rules.size() > 0) {
			TableField newField = new TableField(
					line.split("#")[0].replaceAll("\\s+", ""), schemaName);
			rules.get(rules.size() - 1).addDependant(newField);
		} else {
			throw new DependantWithoutRuleException("Dependant "
					+ line.trim() + " is not attached to a rule");
		}
	}

	void readNewRule(String line) {
		String[] split = line.split("\\s+", 3);

		TableField tableField = new TableField(split[0], schemaName);
		String strategy = split.length > 1 ? split[1] : "";
		String additionalInfo = "";
		if (split.length > 2)
			if (!split[2].startsWith("#"))
				additionalInfo = split[2];
		Rule newRule = new Rule(tableField,
				strategy,
				additionalInfo);
		
		rules.add(newRule);
	}
	
	
	private void readHeader(BufferedReader reader) throws IOException, MalformedException {
		int connectionCount = 0;
		while (true) {
			String line = reader.readLine();
			if (line == null) break;
			if (line.matches("\\s*#.*")) continue;
			if (line.matches("\\s*")) continue;
			
			connectionCount++;
			ConnectionParameters parameters;
			switch (connectionCount) {
			case 1:
				parameters = originalDB;
				break;
			case 2:
				parameters = destinationDB;
				break;
			case 3:
				parameters = transformationDB;
				break;
			default:
				parameters = null;
			}
			if (parameters == null) {
				String[] parts = line.split("\\s+", 2);
				// schema and batch size
				schemaName = parts[0];
				batchSize = Integer.parseInt(parts[1]);
				return;
			} else {
				String[] parts = line.split("\\s+", 3);
				try {
					// url username and password
					parameters.url = parts[0];
					parameters.user = parts[1];
					parameters.password = parts[2];
				} catch (IndexOutOfBoundsException e) {
					throw new MalformedException(
							"invalid database specification: " + line, e);
				}
			}
		}
	}

	public void writeTo(Writer writer) throws IOException {
		writer.write("# originalDB newDB transformationDB each with username password\n");
		for (ConnectionParameters parameters : new ConnectionParameters[] {
				originalDB, destinationDB, transformationDB
		}) {
			writer.write(parameters.url);
			writer.write(" ");
			writer.write(parameters.user);
			writer.write(" ");
			writer.write(parameters.password);
			writer.write("\n");
		}
		writer.write("# schema name and batch size\n");
		writer.write(schemaName);
		writer.write(" ");
		writer.write(Integer.toString(batchSize));
		writer.write("\n\n");
		for (Map.Entry<String, String> pair : strategyMapping.entrySet()) {
			if (NO_OP_STRATEGY_KEY.equals(pair.getKey()))
				continue;
			writer.write(String.format("- %s: %s\n", pair.getKey(), pair.getValue()));
		}
		writer.write("\n# Table.Field\t\tType\t\tAdditionalInfo\n");
		for (Rule rule : rules) {
			if (rule.getDependants().isEmpty() && rule.getStrategy().equals(NO_OP_STRATEGY_KEY))
				writer.write('#');
			writer.write(rule.getTableField().toString());
			if (!rule.getStrategy().equals(NO_OP_STRATEGY_KEY)) {
				writer.write("\t");
				writer.write(rule.getStrategy());
			}
			
			if (!rule.getAdditionalInfo().isEmpty()) {
				writer.write("\t");
				writer.write(rule.getAdditionalInfo());
				writer.write("\n");
			} else
				writer.write("\n");
			
			for (TableField dependent : rule.getDependants()) {
				writer.write("\t" + dependent + "\n");
			}
			
			for (TableField dependent : rule.getPotentialDependants()) {
				writer.write("\t#" + dependent + "\n");
			}
		}
	}
	
	private TransformationStrategy noOpStrategyInstance;

	public Rule addNoOpRuleFor(TableField tableField) {
		Rule newRule = new Rule(
				tableField,
				Config.NO_OP_STRATEGY_KEY, "");
		newRule.setTransformation(noOpStrategyInstance);
		rules.add(newRule);
		return newRule;
	}

	public void removeNoOpRulesWithoutDependants() {
		Iterator<Rule> ruleIterator = rules.iterator();
		while (ruleIterator.hasNext()) {
			Rule rule = ruleIterator.next();
			if (!rule.getStrategy().equals(NO_OP_STRATEGY_KEY))
				continue;
			if (rule.getDependants().isEmpty() && rule.getPotentialDependants().isEmpty())
				ruleIterator.remove();
		}
	}

	public void setRuleTransformations(
			Map<String, TransformationStrategy> strategies) {
		noOpStrategyInstance = strategies.get(NO_OP_STRATEGY_KEY);
		for (Rule rule : rules) {
			rule.setTransformation(strategies.get(rule.getStrategy()));
		}
	}
}
