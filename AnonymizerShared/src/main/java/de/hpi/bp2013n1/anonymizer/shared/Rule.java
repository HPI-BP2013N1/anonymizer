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

public class Rule {
	/**
	 * Origin table and column for this rule. Dependants may be foreign keys
	 * on this attribute.
	 */
	public TableField tableField;
	public String strategy;
	public ArrayList<TableField> dependants = new ArrayList<TableField>();
	public ArrayList<TableField> potentialDependants = new ArrayList<TableField>();
	public String additionalInfo;

	public TableField getTableField() {
		return tableField;
	}
	public String getStrategy() {
		return strategy;
	}
	public ArrayList<TableField> getDependants() {
		return dependants;
	}
	public ArrayList<TableField> getPotentialDependants() {
		return potentialDependants;
	}
	public String getAdditionalInfo() {
		return additionalInfo;
	}
	
	@Override
	public String toString() {
		return tableField + " " + strategy + " " + additionalInfo;
	}
}
