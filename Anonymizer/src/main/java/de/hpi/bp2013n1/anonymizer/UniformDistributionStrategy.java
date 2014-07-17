package de.hpi.bp2013n1.anonymizer;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI Bachelor's Project N1 2013
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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

public class UniformDistributionStrategy extends TransformationStrategy {
	
	class ColumnValueParameters {
		public int targetCardinality = Integer.MAX_VALUE;
		public Map<Object, Integer> existingCardinalities = new HashMap<>();
	}
	
	Map<Rule, ColumnValueParameters> columnValueParameters = new HashMap<>();

	public UniformDistributionStrategy(Anonymizer anonymizer,
			Connection originalDatabase, Connection transformationDatabase)
			throws SQLException {
		super(anonymizer, originalDatabase, transformationDatabase);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules)
			throws PreparationFailedExection {
		for (Rule rule : rules)
			setUpTransformation(rule);
	}

	private void setUpTransformation(Rule rule)
			throws PreparationFailedExection {
		String column = rule.getTableField().column;
		if (!Strings.isNullOrEmpty(rule.additionalInfo)) {
			column = rule.additionalInfo.replace("...", column);
		}
		ColumnValueParameters valueParameters = new ColumnValueParameters();
		try (PreparedStatement groupByStatement = originalDatabase.prepareStatement(
				"SELECT COUNT(*), " + column + " FROM " 
						+ rule.getTableField().schemaTable() 
						+ " GROUP BY " + column);
				ResultSet groupByResult = groupByStatement.executeQuery()) {
			while (groupByResult.next()) {
				int count = groupByResult.getInt(1);
				valueParameters.targetCardinality = 
						valueParameters.targetCardinality > count ? count 
								: valueParameters.targetCardinality;
				valueParameters.existingCardinalities.put(
						groupByResult.getObject(2), count);
			}
		} catch (SQLException e) {
			throw new PreparationFailedExection(
					"Could not retrieve value distribution for " + rule, e);
		}
		columnValueParameters.put(rule, valueParameters);
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws SQLException {
		Object value = oldValue;
		if (!Strings.isNullOrEmpty(rule.additionalInfo))
			value = SQLHelper.selectConstant(originalDatabase,
					rule.additionalInfo.replace("...", "'" + oldValue.toString() + "'"));
		ColumnValueParameters valueParameters = columnValueParameters.get(rule);
		Integer currentCount = valueParameters.existingCardinalities.get(value);
		if (currentCount > valueParameters.targetCardinality
				&& !anonymizer.getRetainService().currentRowShouldBeRetained(
						row.getCurrentSchema(), row.getCurrentTable(), row)) {
			valueParameters.existingCardinalities.put(value, --currentCount);
			return Lists.newArrayList();
		}
		return Lists.newArrayList(oldValue);
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {

	}

	@Override
	public boolean isRuleValid(Rule rule, String typename, int length,
			boolean nullAllowed) throws RuleValidationException {
		return rule.dependants.isEmpty();
	}

}
