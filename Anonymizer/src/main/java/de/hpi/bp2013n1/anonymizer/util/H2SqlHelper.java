package de.hpi.bp2013n1.anonymizer.util;

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


public class H2SqlHelper extends StandardSqlHelper {

	@Override
	public boolean supportsDisableAllForeignKeys() {
		return true;
	}

	@Override
	public String disableAllForeignKeys() {
		return "SET REFERENTIAL_INTEGRITY FALSE";
	}

	@Override
	public String enableAllForeignKeys() {
		return "SET REFERENTIAL_INTEGRITY TRUE";
	}

	@Override
	public String disableForeignKey(String qualifiedTableName,
			String constraintName) {
		return String.format("ALTER TABLE %s SET REFERENTIAL_INTEGRITY FALSE",
				qualifiedTableName);
	}

	@Override
	public String enableForeignKey(String qualifiedTableName,
			String constraintName) {
		return String.format(
				"ALTER TABLE %s SET REFERENTIAL_INTEGRITY TRUE CHECK",
				qualifiedTableName);
	}

}
