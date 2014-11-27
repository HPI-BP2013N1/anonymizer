package de.hpi.bp2013n1.anonymizer.analyzer;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.CombinableMatcher;

import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class RuleMatchers {
	
	static class RuleMatcher extends org.hamcrest.TypeSafeMatcher<Rule> {
		CombinableMatcher<Rule> combination;
		
		private RuleMatcher() {
		}
		
		public static RuleMatcher rule() {
			return new RuleMatcher();
		}
		
		public RuleMatcher forAttribute(Matcher<?> tableField) {
			addProperty("tableField", tableField);
			return this;
		}
		
		public RuleMatcher forAttribute(String tableFieldSpec) {
			return forAttribute(equalTo(new TableField(tableFieldSpec)));
		}
		
		public RuleMatcher withStrategy(String strategy) {
			addProperty("strategy", is(strategy));
			return this;
		}
		
		public RuleMatcher withStrategy(Matcher<?> strategy) {
			addProperty("strategy", strategy);
			return this;
		}
		
		public RuleMatcher withDependants(Matcher<?> dependants) {
			addProperty("dependants", dependants);
			return this;
		}
		
		public RuleMatcher withPotentialDependants(Matcher<?> potentialDependants) {
			addProperty("potentialDependants", potentialDependants);
			return this;
		}
		
		public RuleMatcher withAdditionalInfo(Matcher<?> additionalInfo) {
			addProperty("additionalInfo", additionalInfo);
			return this;
		}

		public RuleMatcher withAdditionalInfo(String string) {
			return withAdditionalInfo(is(string));
		}

		private void addProperty(String name, Matcher<?> matcher) {
			Matcher<Object> hasProperty = hasProperty(name, matcher);
			if (combination == null)
				combination = Matchers.<Rule>both(hasProperty).and(anything());
			else
				combination = combination.and(hasProperty);
		}

		@Override
		public void describeTo(Description description) {
			combination.describeTo(description);
		}

		@Override
		protected boolean matchesSafely(Rule item) {
			return combination == null || combination.matches(item);
		}
	}

	static Matcher<Rule> isLikeRule(
				Matcher<?> tableField,
				Matcher<?> strategy, 
				Matcher<?> dependants,
				Matcher<?> potentialDependants,
				Matcher<?> additionalInfo) {
		return Matchers.<Rule>both(hasProperty("strategy", strategy))
				.and(hasProperty("tableField", tableField))
				.and(hasProperty("dependants", dependants))
				.and(hasProperty("potentialDependants", potentialDependants))
				.and(hasProperty("additionalInfo", additionalInfo));
	}

	static Matcher<Rule> usesStrategy(
				Matcher<?> strategy, 
				Matcher<?> additionalInfo) {
		return Matchers.<Rule>both(hasProperty("strategy", strategy))
				.and(hasProperty("additionalInfo", additionalInfo));
	}

	static Matcher<Rule> appliesStrategyTo(
				Matcher<?> tableField, 
				Matcher<?> strategy,
				Matcher<?> additionalInfo) {
		return Matchers.<Rule>both(hasProperty("strategy", strategy))
				.and(hasProperty("tableField", tableField))
				.and(hasProperty("additionalInfo", additionalInfo));
	}
	
	static Matcher<Rule> ruleForField(Matcher<?> tableField) {
		return hasProperty("tableField", tableField);
	}
	
	static Matcher<Rule> ruleForField(String tableFieldSpec) {
		return ruleForField(equalTo(new TableField(tableFieldSpec)));
	}

}
