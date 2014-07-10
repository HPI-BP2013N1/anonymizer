package de.hpi.bp2013n1.anonymizer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;

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
			ResultSetRowReader row) {
		ColumnValueParameters valueParameters = columnValueParameters.get(rule);
		Integer currentCount = valueParameters.existingCardinalities.get(oldValue);
		if (currentCount > valueParameters.targetCardinality) {
			valueParameters.existingCardinalities.put(oldValue, --currentCount);
			return Lists.newArrayList();
		}
		return Lists.newArrayList(oldValue);
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isRuleValid(Rule rule, String typename, int length,
			boolean nullAllowed) throws RuleValidationException {
		return rule.dependants.isEmpty();
	}

}
