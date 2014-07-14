package de.hpi.bp2013n1.anonymizer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

public class RowMatcher {

	private Connection matchingDatabase;

	public RowMatcher(Connection matchingDatabase) {
		this.matchingDatabase = matchingDatabase;
	}

	boolean rowMatches(Rule rule, ResultSetRowReader row)
			throws SQLException {
		PrimaryKey pk = new PrimaryKey(row.getCurrentSchema(),
				row.getCurrentTable(), matchingDatabase);
		return rowMatches(rule.additionalInfo, row, pk);
	}

	boolean rowMatches(String whereCriterion, ResultSetRowReader row, PrimaryKey pk)
			throws SQLException {
		Map<String, Object> comparisons = pk.whereComparisons(row);
		String wherePKMatches = PrimaryKey.whereComparisonClause(comparisons);
		try (PreparedStatement select = matchingDatabase.prepareStatement(
				rowTestSelectQuery(row.getCurrentSchema(), row.getCurrentTable(), 
						whereCriterion, wherePKMatches))) {
			PrimaryKey.setParametersForPKQuery(comparisons, select);
			try (ResultSet result = select.executeQuery()) {
				if (result.next()) {
					// row matches the specified criteria
					return true;
				}
			}
		}
		return false;
	}

	public String rowTestSelectQuery(String schema, String table, String whereCriterion, String wherePKMatches) {
		return "SELECT 1 FROM " + SQLHelper.qualifiedTableName(schema, table)
				+ " WHERE " + wherePKMatches + " AND (" + whereCriterion + ")";
	}

}
