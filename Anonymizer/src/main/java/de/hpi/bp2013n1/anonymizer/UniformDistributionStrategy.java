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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;
import de.hpi.bp2013n1.anonymizer.util.SafeStringSplitter;

public class UniformDistributionStrategy extends TransformationStrategy {

	static final String PLACEHOLDER = "...";

	static class AdditionalInfo {
		String columnExpressionWithPlaceholder;
		float lowerRowThreshold = 0;
		
		static Pattern lowerThresholdPattern = Pattern.compile(
				"\\s*require (?:(?:at )?min(?:imum)?|at least) (\\d+(?:\\.\\d*)?)(\\s?%)?\\s*",
				Pattern.CASE_INSENSITIVE);
		
		private void privateParse(String additionalInfo, String column) {
			List<String> splitParts = SafeStringSplitter.splitSafely(
				additionalInfo, ';');
			Iterator<String> parts = splitParts.iterator();
			while (parts.hasNext()) {
				String part = parts.next();
				Matcher lowerThresholdMatcher = lowerThresholdPattern.matcher(part);
				if (lowerThresholdMatcher.matches()) {
					lowerRowThreshold = Float.parseFloat(
							lowerThresholdMatcher.group(1));
					boolean isPercentage = lowerThresholdMatcher.group(2) != null;
					if (isPercentage)
						lowerRowThreshold /= 100;
				} else {
					columnExpressionWithPlaceholder = part;
				}
			}
		}
		
		static AdditionalInfo parse(String additionalInfo, String column) {
			AdditionalInfo info = new AdditionalInfo();
			info.privateParse(additionalInfo, column);
			return info;
		}
	}
	
	class ColumnValueParameters {
		public String columnExpression;
		public String columnExpressionWithPlaceholder;
		public long targetCardinality = Long.MAX_VALUE;
		public long lowerThreshold = 0;
		public Map<Object, Long> existingCardinalities = new HashMap<>();
		
		void computeFrom(Rule rule, String column)
				throws PreparationFailedExection {
			columnExpression = column;
			AdditionalInfo info = null;
			if (!Strings.isNullOrEmpty(rule.getAdditionalInfo())) {
				info = AdditionalInfo.parse(rule.getAdditionalInfo(), column);
				if (info.columnExpressionWithPlaceholder != null) {
					columnExpressionWithPlaceholder = info.columnExpressionWithPlaceholder;
					columnExpression = info.columnExpressionWithPlaceholder
							.replace(UniformDistributionStrategy.PLACEHOLDER, column);
				}
			}
			if (info != null && info.lowerRowThreshold >= 1.f) {
				lowerThreshold = (long) Math.ceil(info.lowerRowThreshold);
			}
			try (PreparedStatement groupByStatement = originalDatabase.prepareStatement(
					"SELECT COUNT(*), " + columnExpression + " FROM "
							+ rule.getTableField().schemaTable()
							+ " GROUP BY " + columnExpression);
					ResultSet groupByResult = groupByStatement.executeQuery()) {
				while (groupByResult.next()) {
					long count = groupByResult.getLong(1);
					targetCardinality =
							targetCardinality > count && count >= lowerThreshold
							? count : targetCardinality;
					existingCardinalities.put(
							groupByResult.getObject(2), count);
				}
			} catch (SQLException e) {
				throw new PreparationFailedExection(
						"Could not retrieve value distribution for " + rule, e);
			}
			if (info != null && info.lowerRowThreshold != 0.f) {
				if (info.lowerRowThreshold < 1.f) {
					// percentage
					long max = Ordering.<Long> natural().max(
							existingCardinalities.values());
					lowerThreshold = (long) Math.ceil(
							max * info.lowerRowThreshold);
					targetCardinality = max;
					for (long cardinality : existingCardinalities.values()) {
						if (cardinality >= lowerThreshold
								&& cardinality < targetCardinality)
							targetCardinality = cardinality;
					}
				}
				// else case covered above the database query
			}
		}

		String columnExpressionWithValue(Object oldValue) {
			if (!(oldValue instanceof Number))
				oldValue = "'" + oldValue + "'";
			return columnExpressionWithPlaceholder == null ? oldValue.toString()
					: columnExpressionWithPlaceholder
					.replace(PLACEHOLDER, oldValue.toString());
		}

		boolean needPretransform() {
			return !Strings.isNullOrEmpty(columnExpressionWithPlaceholder);
		}
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
		ColumnValueParameters valueParameters = new ColumnValueParameters();
		valueParameters.computeFrom(rule, column);
		columnValueParameters.put(rule, valueParameters);
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws SQLException {
		Object value = oldValue;
		ColumnValueParameters valueParameters = columnValueParameters.get(rule);
		if (valueParameters.needPretransform())
			value = SQLHelper.selectConstant(originalDatabase,
					valueParameters.columnExpressionWithValue(oldValue));
		long currentCount = valueParameters.existingCardinalities.get(value);
		if (shouldBeRemoved(row, valueParameters, currentCount)) {
			valueParameters.existingCardinalities.put(value, --currentCount);
			return Lists.newArrayList();
		}
		return Lists.newArrayList(oldValue);
	}

	boolean shouldBeRemoved(ResultSetRowReader row,
			ColumnValueParameters valueParameters, long currentCount)
			throws SQLException {
		return (currentCount > valueParameters.targetCardinality
				|| currentCount < valueParameters.lowerThreshold)
				&& !anonymizer.getRetainService().currentRowShouldBeRetained(
						row.getCurrentSchema(), row.getCurrentTable(), row);
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {

	}

	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException {
		// TODO: check SQL selection validity
		return rule.getDependants().isEmpty();
	}

}
