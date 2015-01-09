package de.hpi.bp2013n1.anonymizer.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleConnectorTest {

	private RuleConnector connector;

	@Before
	public void setUp() throws Exception {
		connector = new RuleConnector();
	}

	@Test
	public void testConnectRulesSimple() {
		Rule parent = new Rule(new TableField("S.T1.C1"), "a", "");
		Rule child1 = new Rule(new TableField("S.T2.C1"), "b", "");
		Rule child2 = new Rule(new TableField("S.T3.C1"), "c", "");
		parent.addDependant(child1.getTableField());
		parent.addDependant(child2.getTableField());
		connector.addRules(Arrays.asList(parent, child1, child2));
		connector.connectRules();
		assertThat(parent.getDependentRules(), contains(child1, child2));
		assertThat(child1.getParentRules(), contains(parent));
		assertThat(child2.getParentRules(), contains(parent));
	}

	@Test
	public void testConnectRulesMultipleParents() {
		Rule parent1 = new Rule(new TableField("S.T1.C1"), "a", "");
		Rule parent2 = new Rule(new TableField("S.T1.C1"), "b", "");
		Rule child1 = new Rule(new TableField("S.T2.C1"), "b", "");
		Rule child2 = new Rule(new TableField("S.T3.C1"), "c", "");
		parent1.addDependant(child1.getTableField());
		parent2.addDependant(child2.getTableField());
		connector.addRules(Arrays.asList(parent1, parent2, child1, child2));
		connector.connectRules();
		assertThat(parent1.getDependentRules(), contains(child1, child2));
		assertThat(parent2.getDependentRules(), contains(child1, child2));
		assertThat(child1.getParentRules(), contains(parent1, parent2));
		assertThat(child2.getParentRules(), contains(parent1, parent2));
	}
	
	@Test
	public void testGetRulesBySiteSimple() {
		Rule parent = new Rule(new TableField("S.T1.C1"), "a", "");
		Rule child1 = new Rule(new TableField("S.T2.C1"), "b", "");
		Rule child2 = new Rule(new TableField("S.T3.C1"), "c", "");
		parent.addDependant(child1.getTableField());
		parent.addDependant(child2.getTableField());
		connector.addRules(Arrays.asList(parent, child1, child2));
		Multimap<TableField, Rule> rulesBySite = connector.getRulesBySite();
		assertThat(rulesBySite.keySet(), containsInAnyOrder(
				parent.getTableField(), child1.getTableField(), child2.getTableField()));
		assertThat(rulesBySite.get(parent.getTableField()), contains(parent));
		assertThat(rulesBySite.get(child1.getTableField()), contains(child1));
		assertThat(rulesBySite.get(child2.getTableField()), contains(child2));
	}

	@Test
	public void testGetRulesBySiteMultipleParents() {
		Rule parent1 = new Rule(new TableField("S.T1.C1"), "a", "");
		Rule parent2 = new Rule(new TableField("S.T1.C1"), "b", "");
		assumeThat(parent1.getTableField(), is(parent2.getTableField()));
		Rule child1 = new Rule(new TableField("S.T2.C1"), "b", "");
		Rule child2 = new Rule(new TableField("S.T3.C1"), "c", "");
		parent1.addDependant(child1.getTableField());
		parent2.addDependant(child2.getTableField());
		connector.addRules(Arrays.asList(parent1, parent2, child1, child2));
		Multimap<TableField, Rule> rulesBySite = connector.getRulesBySite();
		assertThat(rulesBySite.keySet(), containsInAnyOrder(
				parent1.getTableField(),
				child1.getTableField(), child2.getTableField()));
		assertThat(rulesBySite.get(parent1.getTableField()),
				contains(parent1, parent2));
		assertThat(rulesBySite.get(parent2.getTableField()),
				contains(parent1, parent2));
		assertThat(rulesBySite.get(child1.getTableField()), contains(child1));
		assertThat(rulesBySite.get(child2.getTableField()), contains(child2));
	}
	
}
