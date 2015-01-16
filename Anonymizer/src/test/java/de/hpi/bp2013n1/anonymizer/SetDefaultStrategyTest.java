package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;

public class SetDefaultStrategyTest {

	SetDefaultStrategy sut;

	@Before
	public void setUp() throws Exception {
		sut = new SetDefaultStrategy(mock(Anonymizer.class), null, null);
	}

	@Test
	public void testIsRuleValid() throws RuleValidationException {
		Rule stringRule = new Rule(new TableField("S.T.C."), "", "abc");
		Rule intRule = new Rule(new TableField("S.T.C"), "", "123");
		Rule nullRule = new Rule(new TableField("S.T.C"), "", "<NULL>");
		Rule defaultRule = new Rule(new TableField("S.T.C"), "", "");
		assertThat("char(3) column should be able to contain 'abc'",
				sut.isRuleValid(stringRule, java.sql.Types.CHAR, 3, false),
				is(true));
		assertThat("varchar(3) column should be able to contain 'abc'",
				sut.isRuleValid(stringRule, java.sql.Types.VARCHAR, 3, false),
				is(true));
		assertThat("varchar(3) column should be able to contain '123'",
				sut.isRuleValid(intRule, java.sql.Types.VARCHAR, 3, false),
				is(true));
		assertThat("varchar(3) column should be able to contain empty string",
				sut.isRuleValid(defaultRule, java.sql.Types.VARCHAR, 3, false),
				is(true));
		assertThat("char(2) column should not be able to contain 'abc'",
				sut.isRuleValid(stringRule, java.sql.Types.CHAR, 2, false),
				is(false));
		assertThat("varchar(2) column should not be able to contain 'abc'",
				sut.isRuleValid(stringRule, java.sql.Types.VARCHAR, 2, false),
				is(false));
		assertThat("varchar(2) column should not be able to contain empty string",
				sut.isRuleValid(defaultRule, java.sql.Types.VARCHAR, 2, false),
				is(true));
		assertThat("integer column should not be able to contain 'abc'",
				sut.isRuleValid(stringRule, java.sql.Types.INTEGER, 8, false),
				is(false));
		assertThat("integer column should be able to contain 123",
				sut.isRuleValid(intRule, java.sql.Types.INTEGER, 8, false),
				is(true));
		assertThat("integer column should not be able to contain empty string",
				sut.isRuleValid(defaultRule, java.sql.Types.INTEGER, 8, false),
				is(false)); // empty string not valid for setValue and int column
		assertThat("nullable column should be able to be nulled",
				sut.isRuleValid(nullRule, java.sql.Types.VARCHAR, 8, true),
				is(true));
		assertThat("nullable integer column should be able to be nulled",
				sut.isRuleValid(nullRule, java.sql.Types.INTEGER, 8, true),
				is(true));
		assertThat("non-nullable column should not be able to be nulled",
				sut.isRuleValid(nullRule, java.sql.Types.VARCHAR, 8, false),
				is(false));
	}

	@Test
	public void testTransformObjectRuleResultSetRowReader() {
		Rule stringRule = new Rule(new TableField("S.T.C."), "", "abc");
		Rule intRule = new Rule(new TableField("S.T.C"), "", "123");
		Rule nullRule = new Rule(new TableField("S.T.C"), "", "<NULL>");
		Rule defaultRule = new Rule(new TableField("S.T.C"), "", "");
		ResultSetRowReader rowReader = mock(ResultSetRowReader.class);
		assertThat(sut.transform("xyz", stringRule, rowReader), contains("abc"));
		assertThat(sut.transform("xyz", nullRule, rowReader), contains(nullValue()));
		assertThat(sut.transform("xyz", intRule, rowReader), contains("123"));
		assertThat(sut.transform("xyz", defaultRule, rowReader), contains(""));
	}

}
