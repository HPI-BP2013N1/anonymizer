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


import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy.PseudonymsTableProxy;
import de.hpi.bp2013n1.anonymizer.db.ColumnDatatypeDescription;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class CharacterStrategy extends TransformationStrategy {

	private Map<Rule, Map<Character, Character>> characterMappings = Maps.newHashMap();
	private Map<Rule, PseudonymsTableProxy> pseudonymTables = Maps.newHashMap();
	private String ignoredCharacters = "";
	
	private Logger characterLogger = Logger.getLogger(CharacterStrategy.class.getName());
	
	public CharacterStrategy(Anonymizer anonymizer, Connection oldDB,
			Connection translateDB) throws SQLException {
		super(anonymizer, oldDB, translateDB);
	}
	
	private String characterMappingTableName(TableField originField) {
		return originField.table + "_" + originField.column + "_CHARACTERS";
	}
	
	private TableField characterMappingTable(TableField originField) {
		return new TableField(characterMappingTableName(originField), null,
				originField.getSchema());
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules)
			throws TransformationTableCreationException, FetchPseudonymsFailedException,
			TransformationKeyCreationException {
		for (Rule configRule : rules)
			setUpTransformation(configRule);
	}
	
	private final ColumnDatatypeDescription singleCharacterColumnDesc =
			new ColumnDatatypeDescription(java.sql.Types.CHAR, 1);
	
	// stubbed in tests
	PseudonymsTableProxy makePseudonymsTableProxy(TableField pseudonymsTableSite,
			ColumnDatatypeDescription columnType, Connection dbConnection) {
		return new PseudonymsTableProxy(pseudonymsTableSite, columnType, dbConnection);
	}

	public void setUpTransformation(Rule rule)
			throws TransformationTableCreationException, FetchPseudonymsFailedException,
			TransformationKeyCreationException {
		PseudonymsTableProxy pseudonymsTable = getPseudonymsTableFor(rule);
		try {
			pseudonymsTable.createIfNotExists();
		} catch (SQLException e1) {
			throw new TransformationTableCreationException(
					"Could not check if pseudonyms table for Rule "
							+ rule + " exists.", e1);
		}
		Map<Character, Character> newCharacterMapping;
		try {
			newCharacterMapping = fillKeyLists(rule);
		} catch (SQLException e) {
			throw new FetchPseudonymsFailedException(e.getMessage());
		}
		try {
			pseudonymsTable.insertNewPseudonyms(newCharacterMapping);
		} catch (SQLException e) {
			throw new TransformationKeyCreationException(
					"Could not insert new character pseudonyms in " +
							pseudonymsTable.getTableSite(), e);
		}
		characterMappings.get(rule).putAll(newCharacterMapping);
	}

	private PseudonymsTableProxy getPseudonymsTableFor(Rule rule) {
		if (pseudonymTables.containsKey(rule))
			return pseudonymTables.get(rule);
		PseudonymsTableProxy pseudonymsTable = makePseudonymsTableProxy(
				characterMappingTable(rule.getTableField()),
				singleCharacterColumnDesc,
				transformationDatabase);
		pseudonymTables.put(rule, pseudonymsTable);
		return pseudonymsTable;
	}

	protected Map<Character, Character> fillKeyLists(Rule rule)
			throws SQLException {
		Map<Character, Character> characterMapping = fetchCharacterMapping(rule);
		Map<Character, Character> newCharacterMapping = fillCharacterMapping(
				Collections.unmodifiableMap(characterMapping));
		return newCharacterMapping;
	}

	public Map<Character, Character> fillCharacterMapping(
			Map<Character, Character> existingMapping) {
		HashMap<Character, Character> newCharacterMapping = new HashMap<>();
		char[] newLowerCaseCharacters;
		char[] newUpperCaseCharacters;
		char[] newNumbers;
		if (!existingMapping.isEmpty()) {
			StringBuilder patternBuilder = new StringBuilder();
			patternBuilder.append('[');
			for (Character c : existingMapping.keySet()) {
				patternBuilder.append(c);
			}
			patternBuilder.append(']');
			newLowerCaseCharacters = lowerCaseCharacters().replaceAll(
					patternBuilder.toString(), "").toCharArray();
			newUpperCaseCharacters = upperCaseCharacters().replaceAll(
					patternBuilder.toString(), "").toCharArray();
			newNumbers = numberCharacters().replaceAll(
					patternBuilder.toString(), "").toCharArray();
		} else {
			newLowerCaseCharacters = lowerCaseCharArray();
			newUpperCaseCharacters = upperCaseCharArray();
			newNumbers = numberArray();
		}
		
		char[] newLowerCasePseudonyms;
		char[] newUpperCasePseudonyms;
		char[] newPseudonymNumbers;
		if (!existingMapping.isEmpty()) {
			StringBuilder usedPseudonymsPattern = new StringBuilder();
			usedPseudonymsPattern.append('[');
			for (Character c : existingMapping.values()) {
				usedPseudonymsPattern.append(c);
			}
			usedPseudonymsPattern.append(']');
			newLowerCasePseudonyms = lowerCaseCharacters().replaceAll(
					usedPseudonymsPattern.toString(), "").toCharArray();
			newUpperCasePseudonyms = upperCaseCharacters().replaceAll(
			usedPseudonymsPattern.toString(), "").toCharArray();
			newPseudonymNumbers = numberCharacters().replaceAll(
			usedPseudonymsPattern.toString(), "").toCharArray();
		} else {
			newLowerCasePseudonyms = lowerCaseCharArray();
			newUpperCasePseudonyms = upperCaseCharArray();
			newPseudonymNumbers = numberArray();
		}
		
		shuffleArrayInPlace(newLowerCasePseudonyms);
		shuffleArrayInPlace(newUpperCasePseudonyms);
		shuffleArrayInPlace(newPseudonymNumbers);
		for (int i = 0; i < newLowerCasePseudonyms.length; i++)
			newCharacterMapping.put(newLowerCaseCharacters[i],
					newLowerCasePseudonyms[i]);
		for (int i = 0; i < newUpperCasePseudonyms.length; i++)
			newCharacterMapping.put(newUpperCaseCharacters[i],
					newUpperCasePseudonyms[i]);
		for (int i = 0; i < newNumbers.length; i++)
			newCharacterMapping.put(newNumbers[i], newPseudonymNumbers[i]);
		return newCharacterMapping;
	}

	private Map<Character, Character> fetchCharacterMapping(Rule rule)
			throws SQLException {
		Map<String, String> mappingInDatabase =
				getPseudonymsTableFor(rule).fetch();
		// there is no "single-character" SQL datatype, so we can only fetch strings
		Map<Character, Character> characterMapping = Maps.newHashMap();
		for (Map.Entry<String, String> entry : mappingInDatabase.entrySet())
			characterMapping.put(entry.getKey().charAt(0), entry.getValue().charAt(0));
		characterMappings.put(rule, characterMapping);
		return characterMapping;
	}
	
	public void setIgnoredCharacters(String toBeIgnored) {
		ignoredCharacters = toBeIgnored;
	}
	
	public String getIgnoredCharacters() {
		return ignoredCharacters;
	}

	@Override
	public List<String> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws TransformationKeyNotFoundException,
			TransformationFailedException {
		checkArgument(oldValue instanceof String || oldValue == null,
				getClass() + " should transform " + oldValue
				+ " but can only opeprate on Strings");
		return Lists.newArrayList(
				transform((String) oldValue, rule,
						rule.getAdditionalInfo().toCharArray()));
	}

	protected String transform(String oldValue, Rule rule, char[] pattern)
			throws TransformationKeyNotFoundException,
			TransformationFailedException {
		StringBuilder newValue = new StringBuilder();
		if (oldValue == null)
			return oldValue;
		for (int i = 0; i < pattern.length; i++) {
			final char patternChar = pattern[i];
			final char originalChar = oldValue.charAt(i);
			switch (patternChar) {
			case 'P':
				if (ignoredCharacters.indexOf(originalChar) != -1) {
					newValue.append(originalChar);
					break;
				}
				Character newChar = getPseudonymCharacterFor(originalChar, rule);
				if (newChar == null)
					throw new TransformationKeyNotFoundException(
							"Could not find pseudonym for character " + originalChar);
				newValue.append(newChar);
				break;
			default:
				newValue.append(originalChar);
				break;
			}
		}
		return newValue.toString();
	}

	private Character getPseudonymCharacterFor(char originalChar, Rule rule) throws TransformationKeyNotFoundException,
			TransformationFailedException {
		Map<Character, Character> mapping = characterMappings.get(rule);
		try {
			if (mapping == null) {
				return getPseudonymsTableFor(rule).fetchOne(originalChar);
			} else {
				Character newChar = mapping.get(originalChar);
				if (newChar == null) {
					return getPseudonymsTableFor(rule).fetchOne(originalChar);
				}
				return newChar;
			}
		} catch (SQLException e) {
			throw new TransformationFailedException(
					"Could not retrieve pseudonym for character " + originalChar,
					e);
		}
	}

	@Override
	public void prepareTableTransformation(TableRuleMap affectedColumnEntries)
			throws SQLException {
		if (affectedColumnEntries.isEmpty())
			return;
		for (Rule rule : affectedColumnEntries.getRules())
			fetchCharacterMapping(rule);
	}

	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) {
		// check for valid type
		if (!SQLTypes.isCharacterType(type)) {
			characterLogger.severe("CharacterStrategy only supports CHAR and VARCHAR fields");
			return false;
		}
		
		// check for additionalInfo present
		if (rule.getAdditionalInfo().length() == 0) {
			characterLogger.severe("CharacterStrategy rules require additionalInfo");
			return false;
		}
		
		// check for additionalInfo length = field length
		if (rule.getAdditionalInfo().length() != length) {
			characterLogger.severe("additionalInfo must be as long as the field (field: " + length + "). Skipping");
			return false;
		}
		
		// check for additionalInfo only P and K
		boolean valid = true;
		for (char c : rule.getAdditionalInfo().toCharArray()) {
			if (c != 'P' && c != 'K') {
				valid = false;
				break;
			}
		}
		if (!valid) {
			characterLogger.severe("additionalInfo can only contain P or K. Skipping");
			return false;
		}
		return true;
	}

}
