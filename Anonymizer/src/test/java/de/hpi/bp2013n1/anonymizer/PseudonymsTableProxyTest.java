package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy.PseudonymsTableProxy;
import de.hpi.bp2013n1.anonymizer.db.ColumnDatatypeDescription;
import de.hpi.bp2013n1.anonymizer.db.TableField;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyCreationException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationKeyNotFoundException;
import de.hpi.bp2013n1.anonymizer.shared.TransformationTableCreationException;

public class PseudonymsTableProxyTest {


	@Test
	public void test() throws SQLException, TransformationTableCreationException, TransformationKeyCreationException, TransformationKeyNotFoundException {
		TableField ptTableField = new TableField("PSEUDONYMS", null, "PUBLIC");
		ColumnDatatypeDescription typeDesc =
				new ColumnDatatypeDescription(java.sql.Types.CHAR, 3);
		Connection testDb = DriverManager.getConnection("jdbc:h2:mem:");
		try {
			PseudonymsTableProxy sut = new PseudonymsTableProxy(ptTableField,
					typeDesc, testDb);
			sut.create();
			try (ResultSet tables = testDb.getMetaData().getTables(
					null, "PUBLIC", "PSEUDONYMS", new String[] { "TABLE" })) {
				assertTrue("Pseudonymization table should have been created",
						tables.next());
			}
			Map<String, String> mapping = Maps.newTreeMap();
			mapping.put("AAA", "XYZ");
			mapping.put("BBB", "HJU");
			mapping.put("CCC", "VVB");
			sut.insertNewPseudonyms(mapping);
			try (PreparedStatement selectStatement = testDb.prepareStatement(
					"SELECT OLDVALUE, NEWVALUE FROM PSEUDONYMS");
					ResultSet pseudonymsResult = selectStatement.executeQuery()) {
				Map<String, String> insertedMapping = Maps.newTreeMap();
				while (pseudonymsResult.next()) {
					insertedMapping.put(pseudonymsResult.getString(1),
							pseudonymsResult.getString(2));
				}
				assertThat("Values in the pseudonyms table should match the "
						+ "inserted mapping", insertedMapping, equalTo(mapping));
			}
			Map<String, String> fetchedMapping = sut.fetchStrings();
			assertThat("Fetched mapping should match the inserted mapping",
					fetchedMapping, equalTo(mapping));
			for (String originalValue : mapping.keySet()) {
				assertThat("Single fetched pseudonym should match the inserted "
						+ "mapping", sut.fetchOneString(originalValue),
						equalTo(mapping.get(originalValue)));
			}
		} finally {
			testDb.close();
		}
	}

}
