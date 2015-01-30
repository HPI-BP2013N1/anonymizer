package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy.PseudonymsTableProxy;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.ColumnTypeNotSupportedException;
import de.hpi.bp2013n1.anonymizer.TransformationStrategy.RuleValidationException;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.Rule;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class PseudonymizeStrategyDBTest {

	private PseudonymizeStrategy sut;
	private Rule rule;
	protected Connection odb;
	protected Connection tdb;
	private String schema;
	private TableField idAttribute;
	private TableField nameAttribute;

	protected void connectToDatabases() throws SQLException {
		odb = DriverManager.getConnection("jdbc:h2:mem:");
		tdb = DriverManager.getConnection("jdbc:h2:mem:");
	}

	@Before
	public void setUp() throws SQLException {
		connectToDatabases();
		try {
			schema = odb.getSchema();
		} catch (AbstractMethodError e) {
			schema = null;
		}
		try (PreparedStatement cts = odb.prepareStatement(
				"CREATE TABLE T (ID INT NOT NULL PRIMARY KEY, NAME CHAR(5) NOT NULL)")) {
			cts.executeUpdate();
		}
		idAttribute = new TableField("T", "ID", schema);
		nameAttribute = new TableField("T", "NAME", schema);
		try (PreparedStatement is = odb.prepareStatement(
				"insert into t (id, name) values (?, ?)")) {
			is.setInt(1, 1);
			is.setString(2, "pete");
			is.addBatch();
			is.setInt(1, 2);
			is.setString(2, "ralph");
			is.addBatch();
			is.executeBatch();
			odb.commit();
		}
		rule = new Rule(new TableField("T", "ID", schema), "", "");
		sut = new PseudonymizeStrategy(null, odb, tdb);
		rule.setTransformation(sut);
	}
	
	@After
	public void tearDown() throws SQLException {
		try {
			try (PreparedStatement cts = odb.prepareStatement(
					"DROP TABLE T")) {
				cts.executeUpdate();
			}
		} finally {
			dropPseudonymsTable();
		}
		odb.close();
		tdb.close();
	}

	private void dropPseudonymsTable() throws SQLException {
		PseudonymsTableProxy pseudonymsTable;
		try {
			pseudonymsTable = sut.getPseudonymsTableFor(rule);
		} catch (SQLException e) {
			// probably does not exist
			return;
		}
		pseudonymsTable.drop();
	}

	@Test
	public void testIntegerSetUpTransformationRule() throws SQLException,
			TransformationKeyCreationException,
			TransformationTableCreationException,
			ColumnTypeNotSupportedException {
		sut.setUpTransformation(Lists.newArrayList(rule));
		// if it fails it should throw SQL errors
	}

	@Test
	public void testIsRuleValid() throws SQLException, RuleValidationException {
		// trivial tests go to PseudonymizeStrategyTest#testIsRuleValid
		Rule prefixRule = new Rule(nameAttribute, "", "P");
		assertThat(sut.isRuleValid(prefixRule, java.sql.Types.CHAR, 5, false),
				is(true));
		assertThat(sut.isRuleValid(prefixRule, java.sql.Types.CHAR, 1, false),
				is(false));
	}
	
	@Test
	public void testIntegerTransform() throws SQLException, TransformationKeyCreationException, TransformationKeyNotFoundException, TransformationTableCreationException {
		// trivial tests go to PseudonymizeStrategyTest#testTransform
		Rule idRule = new Rule(idAttribute, "", "");
		PseudonymsTableProxy pseudonymsTable = sut.getPseudonymsTableFor(idRule);
		Map<Integer, Integer> mapping = Maps.newTreeMap();
		mapping.put(1, 2);
		mapping.put(2, 3);
		pseudonymsTable.create();
		pseudonymsTable.insertNewPseudonyms(mapping);
		
		ResultSetRowReader rowReaderMock = mock(ResultSetRowReader.class);
		assertThat(sut.transform(new Integer(1), idRule, rowReaderMock),
				contains("2"));
		assertThat(sut.transform(new Integer(2), idRule, rowReaderMock),
				contains("3"));
	}
}
