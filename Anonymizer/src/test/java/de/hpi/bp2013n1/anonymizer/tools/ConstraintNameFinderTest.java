package de.hpi.bp2013n1.anonymizer.tools;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

public class ConstraintNameFinderTest {

	private ConstraintNameFinder sut;
	private Connection connection;

	@Before
	public void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:");
		sut = new ConstraintNameFinder(connection);
	}

	@Test
	public void testFindConstraintName() throws SQLException {
		try (Statement ddl = connection.createStatement()) {
			ddl.executeUpdate("CREATE TABLE TABLE1 (A INT PRIMARY KEY)");
			ddl.executeUpdate("CREATE TABLE TABLE2 (A INT, B INT, PRIMARY KEY(A, B))");
			ddl.executeUpdate("CREATE TABLE TABLE3 (A INT, "
					+ "FOREIGN KEY(A) REFERENCES TABLE1(A))");
			ddl.executeUpdate("CREATE TABLE TABLE4 (A INT, B INT, "
					+ "FOREIGN KEY(A, B) REFERENCES TABLE2(A, B))");
			ddl.executeUpdate("CREATE TABLE TABLE5 (A INT, "
					+ "FOREIGN KEY(A) REFERENCES TABLE1(A))");
		}
		assertThat(sut.findConstraintName("TABLE1"), hasSize(2));
		assertThat(sut.findConstraintName("TABLE2"), hasSize(1));
		assertThat(sut.findConstraintName("TABLE3"), hasSize(0));
		assertThat(sut.findConstraintName("TABLE4"), hasSize(0));
		assertThat(sut.findConstraintName("TABLE5"), hasSize(0));
	}

}
