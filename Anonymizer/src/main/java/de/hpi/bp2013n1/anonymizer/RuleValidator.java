package de.hpi.bp2013n1.anonymizer;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleValidator {
	
	private static Logger logger = Logger.getLogger(RuleValidator.class.getName());
	private Map<String, TransformationStrategy> strategies;
	private DatabaseMetaData metaData;

	public RuleValidator(Map<String, TransformationStrategy> strategies,
			DatabaseMetaData metaData) {
		this.strategies = strategies;
		this.metaData = metaData;
	}

	public boolean isValid(Rule rule) {
		String typename = "";
		int length = 0;
		boolean nullAllowed = false;
		if (rule.tableField.column != null) {
			try (ResultSet column = metaData.getColumns(null, 
					rule.tableField.schema, 
					rule.tableField.table,
					rule.tableField.column)) {
				column.next();
				typename = column.getString("TYPE_NAME");
				length = column.getInt("COLUMN_SIZE");
				nullAllowed = column.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
			} catch (SQLException e) {
				logger.severe("Field " + rule.tableField + " does not exist in the schema.");
				return false;
			}
		}
		
		// let the strategies validate their rules
		boolean strategyIsValid = true;
		try {
			strategyIsValid = strategies.get(rule.strategy).isRuleValid(
					rule, typename, length, nullAllowed);
		} catch (RuleValidationException e) {
			logger.severe("Could not validate rule " + rule + ": " 
					+ e.getMessage());
			return false;
		}
		if (!strategyIsValid) {
			return false;
		}
		return true;
	}

}
