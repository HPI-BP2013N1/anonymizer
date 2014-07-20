package de.hpi.bp2013n1.anonymizer.util;

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
