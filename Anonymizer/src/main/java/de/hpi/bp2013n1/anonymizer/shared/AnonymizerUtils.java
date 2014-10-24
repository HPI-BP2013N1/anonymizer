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

import de.hpi.bp2013n1.anonymizer.db.TableField;

public class AnonymizerUtils {
	

	public static ArrayList<TableRuleMap> createTableRuleMaps(
			Config config, Scope scope) {
		ArrayList<TableRuleMap> result = scope.createAllTableRuleMaps();
		for (Rule rule : config.rules) {
			if (!scope.tables.contains(rule.tableField.table))
				continue;
			for (TableRuleMap tableRules : result) {
				if (tableRules.tableName.equals(rule.tableField.table)) {
					tableRules.put(rule.tableField.column, rule);
					break;
				}
			}
			
			for (TableField dependant : rule.dependants) {				
				for (TableRuleMap tableRule : result) {
					if (tableRule.tableName.equals(dependant.table)){
						tableRule.put(dependant.column, rule);
						break;
					}
				}
			}
		}
		return result;
	}

}
