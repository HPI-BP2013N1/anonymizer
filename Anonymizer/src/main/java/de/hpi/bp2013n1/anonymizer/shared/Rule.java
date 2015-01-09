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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import de.hpi.bp2013n1.anonymizer.db.TableField;

public class Rule {
	/**
	 * Origin table and column for this rule. Dependants may be foreign keys
	 * on this attribute.
	 */
	private TableField tableField;
	private String strategy;
	private Set<TableField> dependants = new TreeSet<TableField>();
	private Set<TableField> potentialDependants = new TreeSet<TableField>();
	private String additionalInfo = "";

	public Rule() {
	}
	
	public Rule(TableField tableField, String strategy, String additionalInfo) {
		this.tableField = tableField;
		this.strategy = strategy;
		this.additionalInfo = additionalInfo;
	}
	
	public Rule(TableField tableField, String strategy, String additionalInfo,
			Set<TableField> dependants) {
		this.tableField = tableField;
		this.strategy = strategy;
		this.dependants = dependants;
		this.additionalInfo = additionalInfo;
	}

	public TableField getTableField() {
		return tableField;
	}
	public String getStrategy() {
		return strategy;
	}
	public Set<TableField> getDependants() {
		return dependants;
	}
	
	public boolean addDependant(TableField newDependant) {
		return dependants.add(newDependant);
	}
	
	public Set<TableField> getPotentialDependants() {
		return potentialDependants;
	}
	
	public boolean addPotentialDependant(TableField newPotentialDependant) {
		return potentialDependants.add(newPotentialDependant);
	}
	
	
	public String getAdditionalInfo() {
		return additionalInfo;
	}
	
	@Override
	public String toString() {
		return tableField + " " + strategy + " " + additionalInfo;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((additionalInfo == null) ? 0 : additionalInfo.hashCode());
		result = prime * result
				+ ((dependants == null) ? 0 : dependants.hashCode());
		result = prime * result
				+ ((strategy == null) ? 0 : strategy.hashCode());
		result = prime * result
				+ ((tableField == null) ? 0 : tableField.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Rule other = (Rule) obj;
		if (additionalInfo == null) {
			if (other.additionalInfo != null)
				return false;
		} else if (!additionalInfo.equals(other.additionalInfo))
			return false;
		if (dependants == null) {
			if (other.dependants != null)
				return false;
		} else if (!dependants.equals(other.dependants))
			return false;
		if (strategy == null) {
			if (other.strategy != null)
				return false;
		} else if (!strategy.equals(other.strategy))
			return false;
		if (tableField == null) {
			if (other.tableField != null)
				return false;
		} else if (!tableField.equals(other.tableField))
			return false;
		return true;
	}

}
