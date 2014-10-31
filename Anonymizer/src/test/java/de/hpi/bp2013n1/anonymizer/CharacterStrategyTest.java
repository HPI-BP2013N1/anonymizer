package de.hpi.bp2013n1.anonymizer;

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
