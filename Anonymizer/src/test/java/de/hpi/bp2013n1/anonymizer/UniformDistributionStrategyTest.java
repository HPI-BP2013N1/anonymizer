package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.TransformationStrategy.PreparationFailedExection;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.Scope;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;

public class UniformDistributionStrategyTest {

	private static final int NUMBER_OF_A = 5;
	private static final int NUMBER_OF_B = 1;
	private static final int NUMBER_OF_C = 2;
	private UniformDistributionStrategy sut;
	private TestDataFixture testData;
	private Rule rule;

	@Before
	public void createSubjectUnderTest() throws Exception {
		Config stubConfig = TestDataFixture.makeStubConfig();
		Scope scope = new Scope();
		testData = new TestDataFixture(stubConfig, scope);
		sut = new UniformDistributionStrategy(null,
				testData.originalDbConnection,
				testData.transformationDbConnection);
		try (Statement ddlStatement = testData.originalDbConnection.createStatement()) {
			ddlStatement.executeUpdate("CREATE TABLE aTable ("
					+ "aColumn VARCHAR(20))");
		}
		testData.originalDbConnection.setAutoCommit(false);
		try (PreparedStatement insertStatement = testData.originalDbConnection.prepareStatement(
				"INSERT INTO aTable (aColumn) VALUES (?)")) {
			insertStatement.setString(1, "A");
			for (int i = 0; i < NUMBER_OF_A; i++)
				insertStatement.addBatch();
			insertStatement.setString(1, "B");
			for (int i = 0; i < NUMBER_OF_B; i++)
				insertStatement.addBatch();
			insertStatement.setString(1, "C");
			for (int i = 0; i < NUMBER_OF_C; i++)
				insertStatement.addBatch();
			insertStatement.executeBatch();
			testData.originalDbConnection.commit();
		} finally {
			testData.originalDbConnection.setAutoCommit(true);
		}
		rule = new Rule();
		rule.tableField = new TableField("aTable.aColumn");
		sut.setUpTransformation(Lists.newArrayList(rule));
	}

	@Test
	public void testSetUpTransformation() throws PreparationFailedExection {
		// TODO: implement
	}

	@Test
	public void testTransform() throws SQLException, TransformationKeyNotFoundException {
		assertThat("All tuples from the smallest category should be retained",
				Lists.newArrayList(sut.transform("B", rule, null)),
				hasItems((Object) "B"));
		assertDeletedAndRetained(NUMBER_OF_A, NUMBER_OF_B, "A");
		assertDeletedAndRetained(NUMBER_OF_C, NUMBER_OF_B, "C");
	}

	private void assertDeletedAndRetained(int previousNumber,
			int targetNumber, Object oldValue) throws SQLException,
			TransformationKeyNotFoundException {
		for (int i = 0; i < previousNumber - targetNumber; i++)
			assertThat("First occurences of larger categories should be deleted",
					sut.transform(oldValue, rule, null), 
					emptyIterable());
		for (int i = previousNumber - targetNumber; i < previousNumber; i++)
			assertThat("Later occurences of larger categories should be retained",
					sut.transform(oldValue, rule, null),
					contains(equalTo(oldValue)));
	}

	@Test
	public void testPrepareTableTransformation() {
		// TODO: implement
	}

	@Test
	public void testIsRuleValid() {
		// TODO: implement
	}

}
