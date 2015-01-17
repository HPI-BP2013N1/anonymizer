package de.hpi.bp2013n1.anonymizer;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleValidator {
	
	private static Logger logger = Logger.getLogger(RuleValidator.class.getName());
	private DatabaseMetaData metaData;

	public RuleValidator(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}

	public boolean isValid(Rule rule) {
		int type = 0;
		int length = 0;
		boolean nullAllowed = false;
		TableField tableField = rule.getTableField();
		if (tableField.column != null) {
			try (ResultSet column = metaData.getColumns(null,
					tableField.schema,
					tableField.table,
					tableField.column)) {
				column.next();
				type = column.getInt("DATA_TYPE");
				length = column.getInt("COLUMN_SIZE");
				nullAllowed = column.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
			} catch (SQLException e) {
				logger.severe("Field " + tableField + " does not exist in the schema.");
				return false;
			}
		}
		
		// let the strategies validate their rules
		boolean strategyIsValid = true;
		try {
			strategyIsValid = rule.getTransformation().isRuleValid(
					rule, type, length, nullAllowed);
		} catch (RuleValidationException | NullPointerException e) {
			logger.severe("Could not validate rule " + rule + ": "
					+ e.getMessage());
			return false;
		}
		return strategyIsValid;
	}

}
