package de.hpi.bp2013n1.anonymizer.shared;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.db.TableField;

public class RuleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testTransitiveParents() {
		Rule leaf = new Rule(new TableField("S.ZLEAF.C1"), "", "");
		Rule parent1 = new Rule(new TableField("S.PARENT.C1"), "a", "");
		Rule parent2 = new Rule(new TableField("S.PARENT.C1"), "b", "");
		Rule parentsParent = new Rule(new TableField("S.TOP.C1"), "c", "");
		leaf.addParentRule(parent1);
		leaf.addParentRule(parent2);
		parent1.addParentRule(parentsParent);
		parent2.addParentRule(parentsParent);
		assertThat(parentsParent.transitiveParents(), is(empty()));
		assertThat(parent1.transitiveParents(), contains(parentsParent));
		assertThat(parent2.transitiveParents(), contains(parentsParent));
		assertThat(leaf.transitiveParents(), contains(parentsParent, parent1, parent2));
	}

}
