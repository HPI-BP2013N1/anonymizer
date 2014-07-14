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


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Random;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public abstract class TransformationStrategy {
	
	public class PreparationFailedExection extends Exception {
		private static final long serialVersionUID = 3972841187290919020L;

		public PreparationFailedExection() {
		}

		public PreparationFailedExection(String arg0) {
			super(arg0);
		}

		public PreparationFailedExection(Throwable arg0) {
			super(arg0);
		}

		public PreparationFailedExection(String message, Throwable cause) {
			super(message, cause);
		}

		public PreparationFailedExection(String message, Throwable cause,
				boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}

	}
	
	public static class TransformationFailedException extends Exception {
		public TransformationFailedException() {
			super();
		}

		public TransformationFailedException(String arg0, Throwable arg1,
				boolean arg2, boolean arg3) {
			super(arg0, arg1, arg2, arg3);
		}

		public TransformationFailedException(String arg0, Throwable arg1) {
			super(arg0, arg1);
		}

		public TransformationFailedException(String arg0) {
			super(arg0);
		}

		public TransformationFailedException(Throwable arg0) {
			super(arg0);
		}

		private static final long serialVersionUID = -256002591633736942L;
		
	}

	public static class ColumnTypeNotSupportedException extends Exception {
		private static final long serialVersionUID = -6729775852034148141L;

		public ColumnTypeNotSupportedException(String message) {
			super(message);
		}
	}

	public static class FetchPseudonymsFailedException extends Exception {
		private static final long serialVersionUID = -6838586429361315358L;

		public FetchPseudonymsFailedException(String message) {
			super(message);
		}
	}
	
	public static class RuleValidationException extends Exception {
		private static final long serialVersionUID = -7079792700936251777L;
		
		public RuleValidationException(String message) {
			super(message);
		}
	}

	public static TransformationStrategy loadAndCreate(
			String strategyClassName, Anonymizer anonymizer,
			Connection originalDatabase, Connection transformationDB)
			throws ClassNotFoundException, NoSuchMethodException,
			SecurityException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Class<? extends TransformationStrategy> strategyClass = Class.forName(
				strategyClassName).asSubclass(TransformationStrategy.class);
		Constructor<? extends TransformationStrategy> constructor = strategyClass
				.getConstructor(Anonymizer.class, Connection.class,
						Connection.class);
		return constructor.newInstance(anonymizer, originalDatabase,
				transformationDB);
	}

	protected Connection originalDatabase, transformationDatabase;
	protected Anonymizer anonymizer;

	public TransformationStrategy(Anonymizer anonymizer, 
			Connection originalDatabase, Connection transformationDatabase) 
					throws SQLException {
		this.anonymizer = anonymizer;
		this.originalDatabase = originalDatabase;
		this.transformationDatabase = transformationDatabase;
	}

	abstract public void setUpTransformation(Collection<Rule> rules) 
			throws FetchPseudonymsFailedException, 
			TransformationKeyCreationException, 
			TransformationTableCreationException, ColumnTypeNotSupportedException,
			PreparationFailedExection;

	/**
	 * 
	 * @param oldValue to transform
	 * @param rule Config Rule which said this strategy is to be called
	 * @param row ResultSetRowReader which allows the strategy to access other columns of the original row
	 * @return returns new value if translation was found, returns oldValue otherwise
	 */
	abstract public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws TransformationFailedException,
			SQLException, TransformationKeyNotFoundException;

	public char[] shuffledChars() {
		String allChars = 
				lowerCaseCharacters() + upperCaseCharacters() + numberCharacters();
		return shuffleArray(allChars.toCharArray());
	}

	protected String lowerCaseCharacters() {
		return "qwertzuiopasdfghjklyxcvbnm"; // äöü
	}

	protected String upperCaseCharacters() {
		return lowerCaseCharacters().toUpperCase();
	}

	protected String numberCharacters() {
		return "0123456789";
	}

	protected String specialCharacters() {
		return "ß";
	}

	protected char[] lowerCaseCharacterArray() {
		return lowerCaseCharacters().toCharArray(); // + specialCharacters()
	}

	protected char[] upperCaseCharacterArray() {
		return lowerCaseCharacters().toUpperCase().toCharArray();
	}

	protected char[] numberArray() {
		return numberCharacters().toCharArray();
	}

	public char[] shuffledNumberArray() {
		return shuffleArray(numberArray());
	}

	/**
	 * returns a shuffled copy of the given list
	 * **/

	public char[] shuffleArray(char[] list) {
		char[] shuffledList = list.clone();
		Random rgen = new Random();
		for (int i = 0; i < shuffledList.length; i++) {
			int randomPosition = rgen.nextInt(shuffledList.length);
			Character temp = shuffledList[i];
			shuffledList[i] = shuffledList[randomPosition];
			shuffledList[randomPosition] = temp;
		}
		return shuffledList;
	}

	public abstract void prepareTableTransformation(TableRuleMap tableRules) 
			throws SQLException;
	
	public void printSummary() {
		// no-op by default, subclasses may override this
	}

	public abstract boolean isRuleValid(Rule rule, String typename, int length,
			boolean nullAllowed) throws RuleValidationException;
}
