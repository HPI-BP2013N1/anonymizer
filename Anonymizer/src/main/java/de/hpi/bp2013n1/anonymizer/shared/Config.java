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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.bp2013n1.anonymizer.db.TableField;

public class Config {
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
	public ArrayList<Rule> rules = new ArrayList<Rule>();
	public ConnectionParameters originalDB = new ConnectionParameters();
	public ConnectionParameters destinationDB = new ConnectionParameters();
	public ConnectionParameters transformationDB = new ConnectionParameters();
	public String schemaName;
	public int batchSize;
	public Map<String, String> strategyMapping = new HashMap<>();
	
	private static Logger configLogger = Logger.getLogger(Config.class.getName());
	
	public static Config fromFile(String fileName) throws DependantWithoutRuleException, IOException {
		Config config = new Config();
		config.readFromFile(fileName);
		return config;
	}
	
	public void readFromFile(String filename) throws DependantWithoutRuleException, IOException {		
		File file = new File(filename);
		FileReader fr = new FileReader(file);
		try (BufferedReader reader = new BufferedReader(fr)) {
			read(reader);
		}
	}
	
	public void readFromURL(URL url) throws DependantWithoutRuleException, IOException {
		try (InputStream inputStream = url.openStream();
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
			read(bufferedReader);
		}
	}
	
	public void read(BufferedReader reader)
			throws DependantWithoutRuleException, IOException {
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
		newRule.strategy = split[1];
		
		newRule.additionalInfo = "";
		if(split.length > 2)
			if(!split[2].startsWith("#"))
				newRule.additionalInfo = split[2];
		
		rules.add(newRule);
	}
	
	
	private void readHeader(BufferedReader reader) throws IOException {
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
				// url username and password
				parameters.url = parts[0];
				parameters.user = parts[1];
				parameters.password = parts[2];
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
			for (int j = 0; j < rules.size(); j++) {
				Rule other = rules.get(j);
				for (int k = 0; k < other.dependants.size();) {
					TableField otherDependent = other.dependants.get(k);
					if (rule.tableField.equals(otherDependent)) {
						if (rule.strategy == other.strategy) {
							configLogger.warning(rule.tableField + " is rule, "
									+ "but same-type dependent of " + other.tableField);
							remove = true;							
						} else {
							configLogger.warning(rule.tableField + " is rule, "
									+ "but conflicting dependent of " + other.tableField);
							configLogger.info("Removing dependent entry from " + other.tableField + ".");
							other.dependants.remove(k);
							continue;
						}
					}
					k++;
				}
			}
			
			
			if (remove) {
				configLogger.info("Removing rule.");
				rules.remove(i);
				continue;
			}
			
			// every dependent must occur only once
			for (int j = 0; j < rules.size(); j++) {
				Rule other = rules.get(j);
				if(i > j){
					for (int k = 0; k < rule.dependants.size();) {
						TableField myDependant = rule.dependants.get(k);
						for (int l = 0; l < other.dependants.size(); l++) {
							TableField otherDependant = other.dependants.get(l);
							if(otherDependant.equals(myDependant)){
								configLogger.severe(myDependant + " is dependent of " 
										+ rule.tableField + " and " + other.tableField);
								critical = true;
							}							
						}
						k++;
					}
				}
			}				
					
			i++;
		}
		
		return !critical;		
	}
}
