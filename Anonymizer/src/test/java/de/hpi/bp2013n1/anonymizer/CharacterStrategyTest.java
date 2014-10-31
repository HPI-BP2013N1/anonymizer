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


import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CharacterStrategyTest {
	
	private CharacterStrategy sut;

	@Before
	public void createStrategy() throws SQLException {
		sut = new CharacterStrategy(null, null, null);
	}

	@Test
	public void testFillCharacterMappingRandomness() {
		HashMap<Character, Character> emptyMap = new HashMap<Character, Character>();
		Map<Character, Character> map = sut.fillCharacterMapping(emptyMap);
		Map<Character, Character> previousMap = ImmutableMap.copyOf(map);
		long startTimeMillis = System.currentTimeMillis();
		while (startTimeMillis + 2000 > System.currentTimeMillis() 
				&& previousMap.entrySet().equals(map.entrySet())) {
			previousMap = map;
			map = sut.fillCharacterMapping(emptyMap);
		}
		assertThat("newly generated mappings should not be equal after trying for 2 seconds",
				map, not(equalTo(previousMap)));
	}

}
