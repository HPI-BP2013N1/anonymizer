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
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class DeleteRowStrategy extends TransformationStrategy {

	private RowMatcher criterionMatcher;

	public DeleteRowStrategy(Anonymizer anonymizer,
			Connection originalDatabase, Connection transformationDatabase)
			throws SQLException {
		super(anonymizer, originalDatabase, transformationDatabase);
		criterionMatcher = new RowMatcher(originalDatabase);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules)
			throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedException {
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws SQLException {
		if (criterionMatcher.rowMatches(rule, row))
			return Lists.newArrayList();
		return Lists.newArrayList(oldValue);
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {
	}

	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException {
		boolean valid = new SQLWhereClauseValidator(originalDatabase).
				additionalInfoIsValidWhereClause(rule);
		if (rule.getTableField().getColumn() != null) {
			Logger.getLogger(getClass().getName()).severe("Rule " + rule
					+ " is inavalid because " + getClass().getSimpleName()
					+ " can only be applied to complete rows, not attributes. "
					+ "Remove the attribute name from the rule declaration.");
			valid = false;
		}
		return valid;
	}

}
