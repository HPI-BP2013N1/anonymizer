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
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.RowRetainService.InsertRetainMarkFailed;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.util.SQLHelper;

/**
 * This strategy does not actually transform tuples but it rather marks them
 * to be retained with {@link RowRetainService} if they meet certain criteria.
 * @author Caroline Göricke, Jakob Reschke
 */
public class RetainStrategy extends TransformationStrategy {

	public RetainStrategy(Anonymizer anonymizer, Connection originalDatabase,
			Connection transformationDatabase) throws SQLException {
		super(anonymizer, originalDatabase, transformationDatabase);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules) {
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws SQLException,
			TransformationFailedException {
		try {
			PrimaryKey pk = anonymizer.getRetainService().getPrimaryKey(
					row.getCurrentSchema(), row.getCurrentTable());
			Map<String, Object> comparisons = pk.whereComparisons(row);
			String wherePKMatches = PrimaryKey.whereComparisonClause(comparisons);
			try (PreparedStatement select = originalDatabase.prepareStatement(
					rowTestSelectQuery(row.getCurrentSchema(), 
							row.getCurrentTable(), rule, wherePKMatches))) {
				PrimaryKey.setParametersForPKQuery(comparisons, select);
				try (ResultSet result = select.executeQuery()) {
					if (result.next()) {
						// row matches the specified criteria
						anonymizer.getRetainService().retainCurrentRow(
								row.getCurrentSchema(), 
								row.getCurrentTable(), 
								row);
					}
				}
			}
		} catch (InsertRetainMarkFailed e) {
			throw new TransformationFailedException(e);
		}
		return Lists.newArrayList(oldValue);
	}

	String rowTestSelectQuery(String schema, String table, Rule rule,
			String wherePKMatches) {
		return "SELECT 1 FROM " + SQLHelper.qualifiedTableName(schema, table)
				+ " WHERE " + wherePKMatches + " AND (" + rule.additionalInfo
				+ ")";
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {
	}

	@Override
	public boolean isRuleValid(Rule rule, String typename, int length,
			boolean nullAllowed) throws RuleValidationException {
		try (Statement testSelect = originalDatabase.createStatement()) {
			testSelect.execute("SELECT 1 FROM " + rule.tableField.schemaTable()
					+ " WHERE " + rule.additionalInfo);
		} catch (SQLException e) {
			Logger.getLogger(getClass().getName()).severe("Rule " + rule 
					+ " has an invalid retain criterion: " + e.getMessage());
			return false;
		}
		return true;
	}

}
