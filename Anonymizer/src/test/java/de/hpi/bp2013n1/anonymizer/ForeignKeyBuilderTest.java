package de.hpi.bp2013n1.anonymizer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.junit.Test;

public class ForeignKeyBuilderTest {

	@Test
	public void testSimple() {
		ForeignKey result = new ForeignKeyBuilder("parent")
			.referenceFrom("a").to("b")
			.build();
		assertThat(result.parentTable, is("parent"));
		assertThat(result.parentPrimaryKey, equalTo(new PrimaryKey("b")));
		assertThat(result.foreignKeyColumns, hasEntry("b", "a"));
	}
	
	@Test
	public void testTwo() {
		ForeignKey result = new ForeignKeyBuilder("parent")
		.referenceFrom("a").to("b")
		.referenceFrom("c").to("d")
		.build();
		assertThat(result.parentTable, is("parent"));
		assertThat(result.parentPrimaryKey, equalTo(new PrimaryKey("b", "d")));
		assertThat(result.foreignKeyColumns, hasEntry("b", "a"));
		assertThat(result.foreignKeyColumns, hasEntry("d", "c"));
	}

	@Test
	public void testStaticConstructor() {
		ForeignKey result = ForeignKeyBuilder.withParent("parent")
				.referenceFrom("a").to("b")
				.referenceFrom("c").to("d")
				.build();
		ForeignKey manualResult = new ForeignKeyBuilder("parent")
				.referenceFrom("a").to("b")
				.referenceFrom("c").to("d")
				.build();
		assertThat(result, equalTo(manualResult));
	}
}
