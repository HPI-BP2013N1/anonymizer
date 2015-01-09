package de.hpi.bp2013n1.anonymizer;

/*
 * #%L
 * Anonymizer
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


import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;

public class SetDefaultStrategy extends TransformationStrategy {
	
	static Logger logger = Logger.getLogger(SetDefaultStrategy.class.getName());

	public SetDefaultStrategy(Anonymizer anonymizer, Connection oldDB,
			Connection translateDB) throws SQLException {
		super(anonymizer, oldDB, translateDB);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules) {
	}

	@Override
	public List<String> transform(Object oldValue, Rule rule, ResultSetRowReader row) {
		if (rule.getAdditionalInfo().equals("<NULL>"))
			return null;
		return Lists.newArrayList(rule.getAdditionalInfo());
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {
		// nothing to prepare
	}

	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException {
		// check for defaults given where null not allowed
		if (!nullAllowed && rule.getAdditionalInfo().equals("<NULL>")) {
			logger.severe("This field requires a non-null value. Provide "
					+ "another default value. Skipping");
			return false;
		}
		
		// check for default is valid
		if (SQLTypes.isCharacterType(type)
				&& rule.getAdditionalInfo().length() != 0
				&& rule.getAdditionalInfo().length() > length) {
			logger.severe("Provided default value is longer than maximum field "
					+ "length of " + length + ". Skipping");
			return false;
		}
		return true;
	}
}
