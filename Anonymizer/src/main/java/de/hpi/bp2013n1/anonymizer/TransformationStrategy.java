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

/**
 * Abstract base class for all transformation strategies.
 *
 */
public abstract class TransformationStrategy {
	
	/**
	 * Describes and encapsulates any exceptions that might occur during
	 * preparations of a transformation (in setUpTransformation).
	 */
	public class PreparationFailedException extends Exception {
		private static final long serialVersionUID = 3972841187290919020L;

		public PreparationFailedException() {
		}

		public PreparationFailedException(String arg0) {
			super(arg0);
		}

		public PreparationFailedException(Throwable arg0) {
			super(arg0);
		}

		public PreparationFailedException(String message, Throwable cause) {
			super(message, cause);
		}

		public PreparationFailedException(String message, Throwable cause,
				boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}

	}
	
	/**
	 * Describes and encapsulates any exceptions taht might occur during the
	 * transformation of a value.
	 */
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

	/**
	 * Indicates that a transformation strategy was applied to an attribute with
	 * a type that is not supported by that strategy.
	 */
	public static class ColumnTypeNotSupportedException extends Exception {
		private static final long serialVersionUID = -6729775852034148141L;

		public ColumnTypeNotSupportedException(String message) {
			super(message);
		}
	}

	/**
	 * Indicates that pseudonyms or other data relevant for the transformation
	 * could not be fetched from the transformation database.
	 */
	public static class FetchPseudonymsFailedException extends Exception {
		private static final long serialVersionUID = -6838586429361315358L;

		public FetchPseudonymsFailedException(String message) {
			super(message);
		}

		public FetchPseudonymsFailedException(String message, SQLException cause) {
			super(message, cause);
		}
	}
	
	/**
	 * This exception is thrown if something goes wrong during the validation
	 * of a Rule in an implementation of TransformationStrategy.isRuleValid.
	 */
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

	/**
	 * Prepare transformations according to the supplied rules. This may include
	 * analyzing data in the original database and storing data into the
	 * transformation database. This method is called once per strategy and
	 * Anonymizer run.
	 * 
	 * @param rules Rules which apply this transformation strategy.
	 * @throws FetchPseudonymsFailedException pseudonyms or other transformation
	 * 			data cannot be fetched from the transformation database
	 * @throws TransformationKeyCreationException pseudonyms or other
	 * 			transformation data cannot be inserted into the transformation
	 * 			database
	 * @throws TransformationTableCreationException tables for transformation
	 * 			data could not be created in the transformation database
	 * @throws ColumnTypeNotSupportedException this strategy does not support
	 * 			the type of an attribute referenced by one of the rules
	 * @throws PreparationFailedException anything else went wrong during this
	 * 			set up phase
	 */
	abstract public void setUpTransformation(Collection<Rule> rules)
			throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException, ColumnTypeNotSupportedException,
			PreparationFailedException;

	/**
	 * Transforms a single value. This method is called for every transformed
	 * value from the original database.
	 * 
	 * @param oldValue value to be transformed
	 * @param rule Rule which specifies to apply this strategy to the value
	 * @param row ResultSetRowReader which allows this strategy to access other
	 * 			columns of the original row (which usually contains oldValue)
	 * @return returns an iterable of transformed values. Each element of the
	 * 			iterable will go to a separate row in the destination database.
	 * 			The returned iterable may be empty to indicate that the row from
	 * 			which oldValue was obtained should be deleted.
	 */
	abstract public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws TransformationFailedException,
			SQLException, TransformationKeyNotFoundException;

	public static char[] shuffledChars() {
		String allChars =
				lowerCaseCharacters() + upperCaseCharacters() + numberCharacters();
		return shuffleArray(allChars.toCharArray());
	}

	protected static String lowerCaseCharacters() {
		return "qwertzuiopasdfghjklyxcvbnm"; // äöü
	}

	protected static String upperCaseCharacters() {
		return lowerCaseCharacters().toUpperCase();
	}

	protected static String numberCharacters() {
		return "0123456789";
	}

	protected static String specialCharacters() {
		return "ß";
	}

	protected static char[] lowerCaseCharArray() {
		return lowerCaseCharacters().toCharArray(); // + specialCharacters()
	}

	protected static char[] upperCaseCharArray() {
		return upperCaseCharacters().toCharArray();
	}

	protected static char[] numberArray() {
		return numberCharacters().toCharArray();
	}

	public static char[] shuffledNumberArray() {
		return shuffleArray(numberArray());
	}

	/**
	 * returns a shuffled copy of the given array
	 */
	public static char[] shuffleArray(char[] array) {
		char[] shuffledArray = array.clone();
		shuffleArrayInPlace(shuffledArray);
		return shuffledArray;
	}

	public static void shuffleArrayInPlace(char[] array) {
		Random rgen = new Random();
		for (int i = 0; i < array.length; i++) {
			int randomPosition = rgen.nextInt(array.length);
			Character temp = array[i];
			array[i] = array[randomPosition];
			array[randomPosition] = temp;
		}
	}

	/**
	 * Prepares this strategy to transform values in a single table.
	 * 
	 * @param tableRules TableRuleMap which contains the Rules which state that
	 * 			this strategy is to be applied to the respective columns in the
	 * 			table.
	 * @throws SQLException database errors occured during the preparation
	 * @throws FetchPseudonymsFailedException pseudonyms or other relevant
	 * 			transformation data could not be fetched from the transformation
	 * 			database.
	 */
	public abstract void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException, FetchPseudonymsFailedException;
	
	/**
	 * Prints a summary of this strategy's results. Does nothing by default.
	 */
	public void printSummary() {
		// no-op by default, subclasses may override this
	}

	/**
	 * Checks if a Rule applying this strategy to tables and attributes is valid
	 * (applicable) as far as this strategy is concerned.
	 * 
	 * @param rule Rule which states to apply this strategy to an attribute
	 * @param type SQL type of the attribute to which this strategy is applied
	 * @param length length of the attribute to which this strategy is applied
	 * @param nullAllowed true if SQL NULL is a valid value for this attribute
	 * @return true, if the strategy can be applied to this attribute, false
	 * 			if there are any objections
	 * @throws RuleValidationException an error occurred which prevents a useful
	 * 			validation of this Rule
	 */
	public abstract boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException;
}
