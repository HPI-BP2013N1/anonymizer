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


import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class CharacterStrategy extends TransformationStrategy {

	private Map<Rule, Map<Character, Character>> characterMappings = Maps.newHashMap();	
	private String ignoredCharacters = "";
	
	private Logger characterLogger = Logger.getLogger(CharacterStrategy.class.getName());
	
	public CharacterStrategy(Anonymizer anonymizer, Connection oldDB, 
			Connection translateDB) throws SQLException {
		super(anonymizer, oldDB, translateDB);
	}
	
	private String characterMappingTableName(TableField originField) {
		return originField.table + "_" + originField.column + "_CHARACTERS";
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules) 
			throws TransformationTableCreationException, FetchPseudonymsFailedException, 
			TransformationKeyCreationException {
		for (Rule configRule : rules)
			setUpTransformation(configRule);
	}

	public void setUpTransformation(Rule rule) 
			throws TransformationTableCreationException, FetchPseudonymsFailedException, 
			TransformationKeyCreationException {
		try {
			if (!translationTableExists(rule.tableField))
				createPseudonymsTable(rule.tableField);
		} catch (SQLException e) {
			throw new TransformationTableCreationException(e.getMessage());
		}
		Map<Character, Character> newCharacterMapping;
		try {
			newCharacterMapping = fillKeyLists(rule);
		} catch (SQLException e) {
			throw new FetchPseudonymsFailedException(e.getMessage());
		}
		try {
			insertNewCharacterMapping(rule, newCharacterMapping);
		} catch (SQLException e) {
			throw new TransformationKeyCreationException(e.getMessage());
		}
	}

	private boolean translationTableExists(TableField translatedField) 
			throws SQLException {
		try (ResultSet tableResultSet = 
			transformationDatabase.getMetaData().getTables(
					null,
					translatedField.schema,
					characterMappingTableName(translatedField),
					new String[] { "TABLE" })) {
			return tableResultSet.next(); // next returns true if a row is available
		}
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

	protected void createPseudonymsTable(TableField tableField) 
			throws TransformationTableCreationException {
		String characterMappingSchemaTable = 
				tableField.schema + "." + characterMappingTableName(tableField);
		try (Statement transformationStatement = transformationDatabase.createStatement()) {
			transformationStatement.execute("CREATE TABLE " 
					+ characterMappingSchemaTable 
					+ " (oldValue char(1) NOT NULL, " 
					+ "newValue char(1) NOT NULL, " 
					+ "PRIMARY KEY(oldValue))");
		} catch (SQLException e) {
			if (e.getErrorCode() == -601) {
				characterLogger.info("Creating table " + characterMappingSchemaTable + 
						" failed - already existed");
			} else {
				throw new TransformationTableCreationException(""
						+ "Could not create character transformation table " 
						+ characterMappingSchemaTable
						+ " because of an unknown error.");
			}
		}
	}

	private void insertNewCharacterMapping(Rule rule, 
			Map<Character, Character> newCharacterMapping)
			throws SQLException {
		try (PreparedStatement preparedTranslateStmt = 
				transformationDatabase.prepareStatement(
						"INSERT INTO " + rule.tableField.schema + "." 
						+ characterMappingTableName(rule.tableField) 
						+ " VALUES (?,?)")) {
			for (Map.Entry<Character, Character> pair : newCharacterMapping.entrySet()) {
				preparedTranslateStmt.setString(1, String.valueOf(pair.getKey()));
				preparedTranslateStmt.setString(2, String.valueOf(pair.getValue()));
				preparedTranslateStmt.addBatch();
			}
			preparedTranslateStmt.executeBatch();
			transformationDatabase.commit();
			preparedTranslateStmt.clearBatch();
		}
		characterMappings.get(rule).putAll(newCharacterMapping);
	}

	private Map<Character, Character> fetchCharacterMapping(Rule rule) throws SQLException {
		Map<Character, Character> characterMapping = characterMappings.get(rule);
		if (characterMapping == null) {
			characterMapping = Maps.newHashMap();
			characterMappings.put(rule, characterMapping);
		}
		characterMapping.clear();
		try (PreparedStatement selectStatement = transformationDatabase.prepareStatement(
				"SELECT * FROM " + rule.tableField.schema + "." 
						+ characterMappingTableName(rule.tableField));
				ResultSet mappingResultSet = selectStatement.executeQuery()) {
			while (mappingResultSet.next())
				characterMapping.put(
						mappingResultSet.getString(1).charAt(0), 
						mappingResultSet.getString(2).charAt(0));
		}
		return characterMapping;
	}
	
	public void setIgnoredCharacters(String toBeIgnored) {
		ignoredCharacters = toBeIgnored;
	}
	
	public String getIgnoredCharacters() {
		return ignoredCharacters;
	}

	@Override
	public List<String> transform(Object oldValue, Rule rule, ResultSetRowReader row) throws TransformationKeyNotFoundException {
		checkArgument(oldValue instanceof String || oldValue == null, 
				getClass() + " should transform " + oldValue 
				+ " but can only opeprate on Strings");
		return Lists.newArrayList(
				transform((String) oldValue, rule, 
						rule.additionalInfo.toCharArray()));
	}

	protected String transform(String oldValue, Rule rule, 
			char[] pattern) throws TransformationKeyNotFoundException {
		StringBuilder newValue = new StringBuilder();		
		if (oldValue == null)
			return oldValue;		
		for (int i = 0; i < pattern.length; i++) {
			char c = pattern[i];
			switch (c) {
			case 'P':
				if (ignoredCharacters.indexOf(oldValue.charAt(i)) != -1) {
					newValue.append(oldValue.charAt(i));
					break;
				}
					
				Character newChar = characterMappings.get(rule).get(oldValue.charAt(i));
				if (newChar == null)
					throw new TransformationKeyNotFoundException(
							"No new char found for char." + oldValue.charAt(i));
				newValue.append(newChar);
				break;
			default:
				newValue.append(oldValue.charAt(i));
				break;
			}
		}
		return newValue.toString();
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
		if (rule.additionalInfo.length() == 0) {
			characterLogger.severe("CharacterStrategy rules require additionalInfo");
			return false;
		}
		
		// check for additionalInfo length = field length
		if (rule.additionalInfo.length() != length) {
			characterLogger.severe("additionalInfo must be as long as the field (field: " + length + "). Skipping");
			return false;
		}
		
		// check for additionalInfo only P and K
		boolean valid = true;
		for (char c : rule.additionalInfo.toCharArray()) {
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
