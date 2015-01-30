package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy.PseudonymsTableProxy;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;

public class PseudonymizeStrategyTest {

	private Connection odb;
	private Connection tdb;
	private PseudonymizeStrategy sut;

	@Before
	public void setUp() throws Exception {
		odb = mock(Connection.class, Mockito.RETURNS_MOCKS);
		tdb = mock(Connection.class, Mockito.RETURNS_MOCKS);
		sut = spy(new PseudonymizeStrategy(null, odb, tdb));
	}

	@Test
	public void testIsRuleValid() throws RuleValidationException {
		assertThat(sut.isRuleValid(new Rule(), java.sql.Types.BLOB, 0, true),
				is(false));
		Rule tooLongPrefixRule = new Rule(null, "", "FOO");
		assertThat(sut.isRuleValid(tooLongPrefixRule , java.sql.Types.CHAR, 1, false),
				is(false));
		Rule prefixRule = new Rule(null, "", "P");
		assertThat(sut.isRuleValid(prefixRule, java.sql.Types.INTEGER, 11, true),
				is(false));
	}

	@Test
	public void testTransform() throws SQLException, TransformationKeyNotFoundException {
		PseudonymsTableProxy tableStub = mock(PseudonymsTableProxy.class);
		doReturn(tableStub).when(sut).getPseudonymsTableFor(any(Rule.class));
		when(tableStub.fetchOneString("foo")).thenReturn("bar");
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		assertThat(sut.transform((Object) "foo",
				mock(Rule.class), rowReaderMock),
				contains("bar"));
		when(tableStub.fetchOneString("1")).thenReturn("2");
		assertThat(sut.transform(new Integer(1), mock(Rule.class), rowReaderMock),
				contains("2"));
	}

}
