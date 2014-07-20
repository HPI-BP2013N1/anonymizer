package de.hpi.bp2013n1.anonymizer.util;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class SafeStringSplitterTest {
	
	String input;
	List<String> expectedOutput;
	
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{ "foo", Lists.newArrayList("foo") },
				{ "foo;bar", Lists.newArrayList("foo", "bar") },
				{ "foo ';' bar", Lists.newArrayList("foo ';' bar") },
				{ "foo 'bar'; bla", Lists.newArrayList("foo 'bar'", " bla") },
				{ "foo \";\" bar", Lists.newArrayList("foo \";\" bar") },
				{ "foo \"bar\"; bla", Lists.newArrayList("foo \"bar\"", " bla") },
				{ "foo \";'\"; 'bar'", Lists.newArrayList("foo \";'\"", " 'bar'") },
				{ "foo ';\"'; \"bar\"", Lists.newArrayList("foo ';\"'", " \"bar\"") },
				{ "CONTAINS(..., ';'); MIN 10", Lists.newArrayList("CONTAINS(..., ';')", " MIN 10") },
		});
	}
	
	public SafeStringSplitterTest(String input, List<String> expectedOutput) {
		this.input = input;
		this.expectedOutput = expectedOutput;
	}


	@Test
	public void testSplitSafely() {
		assertThat(SafeStringSplitter.splitSafely(input, ';'),
				contains(expectedOutput.toArray(new String[expectedOutput.size()])));
	}

}
