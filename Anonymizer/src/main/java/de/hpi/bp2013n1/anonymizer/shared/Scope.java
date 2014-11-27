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

public class Scope {
	private static final Charset SCOPE_CHARSET = StandardCharsets.UTF_8;
	public ArrayList<String> tables;

	public static Scope fromFile(String fileName) throws IOException {
		Scope scope = new Scope();
		scope.readFromFile(fileName);
		return scope;
	}
	
	public void readFromFile(String filename) throws IOException{
		try (BufferedReader reader = Files.newBufferedReader(
				FileSystems.getDefault().getPath(filename), SCOPE_CHARSET)) {
			read(reader);
		}
	}
	
	public void readFromURL(URL url) throws IOException {
		try (InputStream inputStream = url.openStream();
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
			read(bufferedReader);
		}
	}

	public void read(BufferedReader reader) throws IOException {
		String line;
		tables = new ArrayList<String>();
		
		while (true) {
			line = reader.readLine();
			if (line == null)
				break;
			if(line.matches("\\s*#.*")) continue;
			tables.add(line);
		}
	}
	
	public ArrayList<TableRuleMap> createAllTableRuleMaps() {
		ArrayList<TableRuleMap> allTableRuleMaps = new ArrayList<TableRuleMap>();
		for (String tableName : tables)
			allTableRuleMaps.add(new TableRuleMap(tableName));
		return allTableRuleMaps;
	}

}
