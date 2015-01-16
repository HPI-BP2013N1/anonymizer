package de.hpi.bp2013n1.anonymizer;

/*
 * #%L
 * Anonymizer
 * %%
 * Copyright (C) 2013 - 2014 HPI Bachelor's Project N1 2013
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


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

import com.google.common.collect.Lists;

import de.hpi.bp2013n1.anonymizer.shared.Config;
import de.hpi.bp2013n1.anonymizer.shared.Config.DependantWithoutRuleException;
import de.hpi.bp2013n1.anonymizer.shared.Config.MalformedException;
import de.hpi.bp2013n1.anonymizer.shared.Scope;

public class StandardTestDataFixture extends TestDataFixture {
	
	public StandardTestDataFixture() throws IOException, DependantWithoutRuleException, ClassNotFoundException, SQLException, MalformedException  {
		readConfigAndScope();
		createDbConnections();
	}

	public StandardTestDataFixture(Config config, Scope scope)
			throws ClassNotFoundException, IOException, SQLException, DependantWithoutRuleException, MalformedException {
		super(config, scope);
	}

	public StandardTestDataFixture(Connection originalDbConnection,
			Connection destinationDbConnection,
			Connection transformationDbConnection) throws IOException, DependantWithoutRuleException, MalformedException {
		readConfigAndScope();
		this.originalDbConnection = originalDbConnection;
		this.destinationDbConnection = destinationDbConnection;
		this.transformationDbConnection = transformationDbConnection;
	}

	@Override
	protected URL getScopeURL() {
		return AnonymizerCompleteTest.class.getResource("testscope.txt");
	}

	@Override
	protected URL getConfigURL() {
		return StandardTestDataFixture.class.getResource("test-h2-config.txt");
	}

	@Override
	protected InputStream getOriginalDataSet() {
		return StandardTestDataFixture.class.getResourceAsStream("testdata.xml");
	}

	@Override
	protected InputStream getTransformationDataSet() {
		return StandardTestDataFixture.class.getResourceAsStream("testpseudonyms.xml");
	}

	@Override
	protected Iterable<InputStream> getDDLs() {
		return Lists.newArrayList(
				StandardTestDataFixture.class.getResourceAsStream("testschema.ddl.sql"));
	}

	@Override
	protected Iterable<InputStream> getTearDownSQL() {
		return Lists.newArrayList(
				StandardTestDataFixture.class.getResourceAsStream("testschema.teardown.sql"));
	}

	@Override
	protected Iterable<InputStream> getTransformationDDLs() {
		return Lists.newArrayList(
				StandardTestDataFixture.class.getResourceAsStream("testpseudonymsschema.ddl.sql"));
	}

	@Override
	protected Iterable<InputStream> getTransformationTearDownSQL() {
		return Lists.newArrayList(
				StandardTestDataFixture.class.getResourceAsStream("testpseudonymsschema.teardown.sql"));
	}

	@Override
	protected IDataSet expectedDestinationDataSet() throws DataSetException {
		FlatXmlDataSetBuilder dataSetBuilder = new FlatXmlDataSetBuilder();
		return dataSetBuilder.build(StandardTestDataFixture.class
				.getResourceAsStream("transformed-testdata.xml"));
	}
}
