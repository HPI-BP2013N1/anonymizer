package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RowMatcherTest {

	private RowMatcher sut;

	@Before
	public void createMatcher() {
		sut = new RowMatcher(null);
	}

	@Test
	public void testRowTestSelectQuery() {
		String criterion = "A = 'A' OR B = 'B'";
		assertThat(sut.rowTestSelectQuery("S", "T", criterion, "P1 = ? AND P2 = ?"),
				equalTo("SELECT 1 FROM S.T "
						+ "WHERE P1 = ? AND P2 = ? AND (A = 'A' OR B = 'B')"));
	}

}
