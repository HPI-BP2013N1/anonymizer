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

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.RowRetainService.InsertRetainMarkFailed;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;

/**
 * This strategy does not actually transform tuples but it rather marks them
 * to be retained with {@link RowRetainService} if they meet certain criteria.
 * @author Caroline Göricke, Jakob Reschke
 */
public class RetainRowStrategy extends TransformationStrategy {
	
	private RowMatcher criterionMatcher;

	public RetainRowStrategy(Anonymizer anonymizer, Connection originalDatabase,
			Connection transformationDatabase) throws SQLException {
		super(anonymizer, originalDatabase, transformationDatabase);
		criterionMatcher = new RowMatcher(originalDatabase);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules) {
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws SQLException,
			TransformationFailedException {
		try {
			if (criterionMatcher.rowMatches(rule, row))
				anonymizer.getRetainService().retainCurrentRow(
					row.getCurrentSchema(), 
					row.getCurrentTable(), 
					row);
		} catch (InsertRetainMarkFailed e) {
			throw new TransformationFailedException(e);
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
		return new SQLWhereClauseValidator(originalDatabase).
				additionalInfoIsValidWhereClause(rule);
	}

}
