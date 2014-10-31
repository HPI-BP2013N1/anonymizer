package de.hpi.bp2013n1.anonymizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TableRuleMap;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class NoOperationStrategy extends TransformationStrategy {

	public NoOperationStrategy(Anonymizer anonymizer,
			Connection originalDatabase, Connection transformationDatabase)
			throws SQLException {
		super(anonymizer, originalDatabase, transformationDatabase);
	}

	@Override
	public void setUpTransformation(Collection<Rule> rules)
			throws FetchPseudonymsFailedException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException, PreparationFailedExection {
	}

	@Override
	public Iterable<?> transform(Object oldValue, Rule rule,
			ResultSetRowReader row) throws TransformationFailedException,
			SQLException, TransformationKeyNotFoundException {
		return Lists.newArrayList(oldValue);
	}

	@Override
	public void prepareTableTransformation(TableRuleMap tableRules)
			throws SQLException {
	}

	@Override
	public boolean isRuleValid(Rule rule, int type, int length,
			boolean nullAllowed) throws RuleValidationException {
		return true;
	}

}
