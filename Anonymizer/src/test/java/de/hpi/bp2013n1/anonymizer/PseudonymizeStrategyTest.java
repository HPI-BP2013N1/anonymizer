package de.hpi.bp2013n1.anonymizer;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI-BP2013N1
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy;

public class PseudonymizeStrategyTest {

	private PseudonymizeStrategy sut;

	@Before
	public void createStrategyObject() throws Exception {
		Connection stubDB = DriverManager.getConnection("jdbc:h2:mem:");
		sut = new PseudonymizeStrategy(null, stubDB, stubDB);
	}

	@Test
	public void testCreateRandomPseudonym() {
		final String PREFIX = "foo";
		String randomPseudonym = sut.createPseudonym(0, 2, PREFIX);
		assertThat("Required length and prefix length should add up.",
				randomPseudonym.length(), is(5));
		assertThat("The generated pseuodnym should start with the prefix.",
				randomPseudonym, startsWith(PREFIX));
	}

	@Test
	public void testCreateRandomPseudonyms() {
		List<String> randomPseudonyms = sut.createRandomPseudonyms(100, 2, "");
		assertThat(randomPseudonyms, hasSize(100));
		for (String p : randomPseudonyms)
			assertThat(p.length(), is(2));
		for (int length = 1; length < 2; length++) {
			int count = (int) Math.pow(
					PseudonymizeStrategy.NUMBER_OF_AVAILABLE_CHARS, length);
			randomPseudonyms = sut.createRandomPseudonyms(count, length, "");
			assertThat(new HashSet<>(randomPseudonyms), hasSize(count));
		}
	}

	@Test
	public void testCreateRandomIntegers() {
		Collection<String> randomIntegerStrings = 
				sut.createIntegers(100, 2);
		assertThat(randomIntegerStrings, hasSize(100));
		assertThat(new HashSet<>(randomIntegerStrings), hasSize(100));
		for (String intStr : randomIntegerStrings)
			assertThat(intStr.length(), is(2));
	}

}
