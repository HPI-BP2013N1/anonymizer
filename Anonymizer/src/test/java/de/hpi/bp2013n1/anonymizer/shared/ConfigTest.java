package de.hpi.bp2013n1.anonymizer.shared;

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


import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import de.hpi.bp2013n1.anonymizer.NoOperationStrategy;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;

public class ConfigTest {
	
	private Config sut;

	@Before
	public void setUpConfig() {
		sut = new Config();
	}

	@Test
	public void testReadDependant() throws DependantWithoutRuleException {
		sut.readNewRule("Table.Column Strategy Addinfo");
		sut.readDependant("\tTable2.Column2");
		assertThat(sut.rules.get(sut.rules.size() - 1).getDependants(),
				hasItem(new TableField("Table2.Column2")));
		sut.readDependant("  Table3.c");
		assertThat(sut.rules.get(sut.rules.size() - 1).getDependants(),
				hasItem(new TableField("Table3.c")));
		sut.readDependant(" Table4.c # comment");
		assertThat(sut.rules.get(sut.rules.size() - 1).getDependants(),
				hasItem(new TableField("Table4.c")));
	}

	@Test
	public void testReadNewRule() {
		sut.readNewRule("Table.Column Strategy AddInfo");
		Rule rule = new Rule(new TableField("Table.Column"), "Strategy", "AddInfo");
		assertThat(sut.rules, hasItem(rule));
		sut.readNewRule("Table.Column Strategy");
		rule = new Rule(new TableField("Table.Column"), "Strategy", "");
		assertThat(sut.rules, hasItem(rule));
		sut.readNewRule("Table Strategy AddInfo");
		rule = new Rule(new TableField("Table"), "Strategy", "AddInfo");
		assertThat(sut.rules, hasItem(rule));
		sut.readNewRule("Table Strategy additional info");
		rule = new Rule(new TableField("Table"), "Strategy", "additional info");
		assertThat(sut.rules, hasItem(rule));
	}
	
	private static String simpleConfig = "urlsrc user pw\n"
			+ "urldst user pw\n"
			+ "urltrf user pw\n"
			+ "schema 1000\n"
			+ "- S1: strategy1\n"
			+ "- S2: strategy2\n"
			+ "Table.Column S1 foo\n"
			+ "  Table2.Column\n"
			+ "Table2.Column2 S2\n";
	
	@Test
	public void testSimpleRead() throws IOException, DependantWithoutRuleException, MalformedException {
		try (StringReader stringReader = new StringReader(simpleConfig);
				BufferedReader reader = new BufferedReader(stringReader)) {
			sut.read(reader);
		}
		assertThat(sut.originalDB.url, is("urlsrc"));
		assertThat(sut.originalDB.user, is("user"));
		assertThat(sut.originalDB.password, is("pw"));
		assertThat(sut.destinationDB.url, is("urldst"));
		assertThat(sut.transformationDB.url, is("urltrf"));
		assertThat(sut.schemaName, is("schema"));
		assertThat(sut.batchSize, is(1000));
		assertThat(sut.strategyMapping, hasEntry("S1", "strategy1"));
		assertThat(sut.strategyMapping, hasEntry("S2", "strategy2"));
		assertThat(sut.strategyMapping, hasEntry(Config.NO_OP_STRATEGY_KEY, NoOperationStrategy.class.getName()));
		assertThat(
				sut.rules,
				contains(
						new Rule(new TableField("Table", "Column", "schema"),
								"S1", "foo", Sets.newHashSet(new TableField(
										"Table2", "Column", "schema"))),
						new Rule(new TableField("Table2", "Column2", "schema"),
								"S2", "")));
	}

}
