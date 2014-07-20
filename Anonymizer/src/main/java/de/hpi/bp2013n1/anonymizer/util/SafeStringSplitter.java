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


import java.util.List;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;

public abstract class SafeStringSplitter {

	public static List<String> splitSafely(String input, char splitChar) {
		List<String> splitParts = Lists.newArrayList(input.split(
				Character.toString(splitChar)));
		CharMatcher quoteMatcher = CharMatcher.anyOf("'\"");
		for (int i = 0; i < splitParts.size(); i++) {
			StringBuilder currentPart = new StringBuilder(splitParts.get(i));
			int start = 0;
			boolean sq = false, dq = false;
			while (true) {
				int position = quoteMatcher.indexIn(currentPart, start);
				if (position == -1) {
					if (sq || dq) {
						currentPart.append(splitChar).append(splitParts.remove(i + 1));
						// TODO: catch IndexOutOfBoundsException => unbalanced
						continue;
					} else {
						splitParts.set(i, currentPart.toString());
						break;
					}
				}
				switch (currentPart.charAt(position)) {
				case '\'':
					if (!dq)
						sq = !sq;
					break;
				case '"':
					if (!sq)
						dq = !dq;
					break;
				}
				start = position + 1;
			}
		}
		return splitParts;
	}

}
