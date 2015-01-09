package de.hpi.bp2013n1.anonymizer.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleConnector {
	List<Rule> rules = new ArrayList<>();
	Multimap<TableField, Rule> rulesBySite = ArrayListMultimap.create();
	String defaultStrategy = Config.NO_OP_STRATEGY_KEY;
	
	public boolean addRules(Collection<Rule> newRules) {
		return rules.addAll(newRules);
	}
	
	public void connectRules() {
		initializeRulesBySite();
		synchronizeDependents();
		for (Rule eachRule : rules) {
			for (TableField eachDependent : eachRule.getDependants()) {
				for (Rule eachDependentRule : rulesBySite.get(eachDependent)) {
					eachDependentRule.addParentRule(eachRule);
					eachRule.addDependentRule(eachDependentRule);
				}
			}
		}
	}
	
	public Multimap<TableField, Rule> getRulesBySite() {
		connectRules();
		return ImmutableMultimap.copyOf(rulesBySite);
	}

	private void synchronizeDependents() {
		for (Map.Entry<TableField, Collection<Rule>> siteAndRules : rulesBySite.asMap().entrySet()) {
			Collection<Rule> rulesForThisSite = siteAndRules.getValue();
			Set<TableField> allDependants = new TreeSet<>();
			for (Rule eachRule : rulesForThisSite) {
				allDependants.addAll(eachRule.getDependants());
			}
			for (Rule eachRule : rulesForThisSite) {
				eachRule.setDependants(allDependants);
			}
		}
	}

	private void initializeRulesBySite() {
		for (Rule eachRule : rules) {
			rulesBySite.put(eachRule.getTableField(), eachRule);
		}
		for (Rule eachRule : rules) {
			for (TableField eachDependent : eachRule.getDependants()) {
				if (rulesBySite.get(eachDependent).isEmpty())
					rulesBySite.put(eachDependent, newStubRuleFor(eachDependent));
			}
		}
	}

	private Rule newStubRuleFor(TableField tableField) {
		return new Rule(tableField, defaultStrategy, "");
	}
}
