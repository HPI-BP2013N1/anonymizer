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


import java.util.ArrayList;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

public class TableRuleMap {
	public String tableName;
	private Multimap<String, Rule> columnRules = ArrayListMultimap.create();
	
	public TableRuleMap(String tableName) {
		this.tableName = tableName;
	}
	
	public void put(String columnName, Rule configRule) {
		columnRules.put(columnName, configRule);
	}

	public ImmutableList<Rule> getRules(String column) {
		return ImmutableList.copyOf(columnRules.get(column));
	}
	
	public ImmutableList<Rule> getRulesIgnoreCase(String column) {
		ArrayList<Rule> rules = new ArrayList<>();
		for (Map.Entry<String, Rule> entry : columnRules.entries())
			if (entry.getKey().equalsIgnoreCase(column))
				rules.add(entry.getValue());
		return ImmutableList.copyOf(rules);
	}
	
	public TableRuleMap filteredByStrategy(String strategyClassName) {
		TableRuleMap filteredMap = new TableRuleMap(tableName);
		for (Map.Entry<String, Rule> entry : columnRules.entries())
			if (entry.getValue().strategy.equals(strategyClassName))
				filteredMap.put(entry.getKey(), entry.getValue());
		return filteredMap;
	}
	
	public ImmutableSet<String> getColumnNames() {
		return ImmutableSet.copyOf(columnRules.keySet());
	}
	
	public ImmutableList<Rule> getRules() {
		return ImmutableList.copyOf(columnRules.values());
	}

	public boolean isEmpty() {
		return columnRules.isEmpty();
	}
}
