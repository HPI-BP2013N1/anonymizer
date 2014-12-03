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
			rules.get(rules.size() - 1).dependants.add(newField);
		} else {
			throw new DependantWithoutRuleException("Dependant "
					+ line.trim() + " is not attached to a rule");
		}
	}

	void readNewRule(String line) {
		String[] split = line.split("\\s+", 3);

		Rule newRule = new Rule();
		newRule.tableField = new TableField(split[0], schemaName);
		newRule.strategy = split.length > 1 ? split[1] : "";
		
		newRule.additionalInfo = "";
		if(split.length > 2)
			if(!split[2].startsWith("#"))
				newRule.additionalInfo = split[2];
		
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

	// Remove all root nodes that are actually dependents of other root nodes
	// and give a warning
	public boolean validate() {
		boolean critical = false;
		
		for (int i = 0; i < rules.size();) {
			Rule rule = rules.get(i);
			boolean remove = false;
			
			// rule must not be dependent
			for (Rule other : rules) {
				Iterator<TableField> otherDependantsIterator =
						other.dependants.iterator();
				while (otherDependantsIterator.hasNext()) {
					TableField otherDependent = otherDependantsIterator.next();
					if (rule.tableField.equals(otherDependent)) {
						if (rule.strategy.equals(other.strategy)) {
							configLogger.warning(rule.tableField + " is "
									+ "dependent on " + other.tableField
									+ "but also has an own rule applying the same "
									+ "transformation. Changing transformation "
									+ "of the latter rule to none.");
							rule.strategy = NO_OP_STRATEGY_KEY;
						} else {
							// TODO: this effectively forbids composed rules A <- B <- C, change it (INNO-151)?
							configLogger.warning(rule.tableField + " is rule, "
									+ "but conflicting dependent of " + other.tableField);
							configLogger.info("Removing dependent entry from " + other.tableField + ".");
							otherDependantsIterator.remove();
							continue;
						}
					}
				}
			}
			
			
			if (remove) {
				configLogger.info("Removing rule " + rule);
				rules.remove(i);
				continue;
			}
					
			i++;
		}
		
		return !critical;
	}
}
