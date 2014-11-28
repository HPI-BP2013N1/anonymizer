package de.hpi.bp2013n1.anonymizer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;

public class TestSpecificTestDataFixture extends TestDataFixture {
	private String filenamePrefix = "";
	private Object testObject;

	public TestSpecificTestDataFixture(Object testObject, String filenamePrefix) throws IOException,
			DependantWithoutRuleException, ClassNotFoundException, SQLException, MalformedException {
		this.testObject = testObject;
		this.filenamePrefix = filenamePrefix;
		readConfigAndScope();
		createDbConnections();
	}
	
	public TestSpecificTestDataFixture(Object testObject) throws IOException,
			DependantWithoutRuleException, ClassNotFoundException, SQLException, MalformedException {
		this(testObject, testObject.getClass().getSimpleName());
	}
	
	InputStream getResourceAsStream(String suffix) {
		return testObject.getClass().getResourceAsStream(filenamePrefix + suffix);
	}
	
	URL getResource(String suffix) {
		return testObject.getClass().getResource(filenamePrefix + suffix);
	}
	
	@Override
	protected URL getConfigURL() {
		return getResource("-config.txt");
	}
	
	@Override
	protected URL getScopeURL() {
		return getResource("-scope.txt");
	}
	
	@Override
	protected Iterable<InputStream> getDDLs() {
		return Lists.newArrayList(getResourceAsStream("-ddl.sql"));
	}

	@Override
	protected Iterable<InputStream> getTearDownSQL() {
		return Lists.newArrayList(getResourceAsStream("-teardown.sql"));
	}

	@Override
	protected Iterable<InputStream> getTransformationDDLs() {
		return Lists.newArrayList();
	}

	@Override
	protected Iterable<InputStream> getTransformationTearDownSQL() {
		return Lists.newArrayList();
	}
	
	@Override
	protected InputStream getOriginalDataSet() {
		return getResourceAsStream("-data.xml");
	}
	
	@Override
	protected InputStream getTransformationDataSet() {
		return null;
	}
	
	@Override
	protected IDataSet expectedDestinationDataSet() throws DataSetException {
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		return dataSetBuilder.build(getResourceAsStream("-resultdata.xml"));
	}
}