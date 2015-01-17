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


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.analyzer.Analyzer.FatalError;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.DatabaseConnector;
import de.hpi.bp2013n1.anonymizer.shared.Scope;


class Reference {
	String tableName;
	String[] fkColnames;
	String[] pkColnames;
}


public class Main {
	
	private static Logger logger = Logger.getLogger(Main.class.getName());


	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Expected 3 Arguments\n"
					+ "1. : path to config file\n"
					+ "2. : path to scope\n"
					+ "3. : path to output config file");
			System.exit(64);
			return;
		}
		
		Config config = new Config();
		Scope scope = new Scope();
		Connection con;
		Analyzer analyzer;

		try {
			config.readFromFile(args[0]);
		} catch (IOException e) {
			logger.severe("Could not read from config file: " + e.getMessage());
			System.exit(74);
			return;
		}
		catch (DependantWithoutRuleException | MalformedException e) {
			logger.severe("Invalid configuration: " + e.getMessage());
			System.exit(78);
			return;
		}

		try {
			scope.readFromFile(args[1]);
		} catch (IOException e) {
			logger.severe("Could not read from scope file: " + e.getMessage());
			System.exit(74);
			return;
		}
		
		try {
			con = DatabaseConnector.connect(config.originalDB);
			
			analyzer = new Analyzer(con, config, scope);
			analyzer.run(args[2]);
		} catch (FatalError e) {
			logger.severe("Cannot recover from previous error, exiting.");
			System.exit(1);
		} catch (SQLException e) {
			logger.severe("Could not connect to the original database.");
			System.exit(1);
		}
	}

}
