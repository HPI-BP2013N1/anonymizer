package de.hpi.bp2013n1.anonymizer.analyzer;

import static de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.RuleMatcher.rule;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.junit.Assume;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.analyzer.RuleMatchers.RuleMatcher;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleMatcherTest {

	@Test
	public void testRule() {
		RuleMatcher isMatched = rule();
		assertThat(new Rule(), isMatched);
	}

	@Test
	public void testForAttributeString() {
		RuleMatcher isMatched = rule().forAttribute("TABLE.ATTRIBUTE");
		assertThat(new Rule(new TableField("TABLE.ATTRIBUTE"), null, null),
				isMatched);
	}

	@Test
	public void testWithStrategyString() {
		RuleMatcher isMatched = rule().withStrategy("X");
		assertThat(new Rule(null, "X", null), isMatched);
	}

	@Test
	public void testWithDependants() {
		Rule testedRule = new Rule();
		RuleMatcher isMatched = rule().withDependants(empty());
		Assume.assumeThat(testedRule.getDependants(), empty());
		assertThat(testedRule, isMatched);
		
		isMatched = rule().withDependants(contains(new TableField("A.B")));
		testedRule.addDependant(new TableField("A.B"));
		assertThat(testedRule, isMatched);
		
		isMatched = rule().withDependants(
				containsInAnyOrder(new TableField("A.B"), new TableField("C.D")));
		testedRule.addDependant(new TableField("C.D"));
		assertThat(testedRule, isMatched);
	}

	@Test
	public void testWithPotentialDependants() {
		Rule testedRule = new Rule();
		RuleMatcher isMatched = rule().withPotentialDependants(empty());
		Assume.assumeThat(testedRule.getPotentialDependants(), empty());
		assertThat(testedRule, isMatched);
		
		isMatched = rule().withPotentialDependants(contains(new TableField("A.B")));
		testedRule.addPotentialDependant(new TableField("A.B"));
		assertThat(testedRule, isMatched);
		
		isMatched = rule().withPotentialDependants(
				containsInAnyOrder(new TableField("A.B"), new TableField("C.D")));
		testedRule.addPotentialDependant(new TableField("C.D"));
		assertThat(testedRule, isMatched);
	}

	@Test
	public void testWithAdditionalInfo() {
		RuleMatcher isMatched = rule().withAdditionalInfo("X");
		assertThat(new Rule(null, null, "X"), isMatched);
	}
	
	@Test
	public void complexTest() {
		Rule testedRule = new Rule(new TableField("A.B"), "strategy", "info");
		testedRule.addDependant(new TableField("C.D"));
		testedRule.addDependant(new TableField("C.E"));
		testedRule.addPotentialDependant(new TableField("E.F"));
		RuleMatcher isMatched = rule().forAttribute("A.B")
				.withStrategy("strategy").withAdditionalInfo("info")
				.withDependants(hasItem(new TableField("C.D")))
				.withPotentialDependants(contains(new TableField("E.F")));
		assertThat(testedRule, isMatched);
	}

}
