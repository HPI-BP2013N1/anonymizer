package de.hpi.bp2013n1.anonymizer;

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
