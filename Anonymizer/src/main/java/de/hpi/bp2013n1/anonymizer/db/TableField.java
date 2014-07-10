package de.hpi.bp2013n1.anonymizer.db;

/*
 * #%L
 * AnonymizerShared
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


public class TableField {
	public String table;
	public String column;
	public String schema;

	public TableField(String str){
			String[] split = str.split("\\.");
			table = split[0];
			column = split[1];
	}
	
	public TableField(String str, String schemaName){
		String[] split = str.split("\\.");
		table = split[0];
		column = split[1];
		this.schema = schemaName;
	}
	
	public TableField(String table, String field, String schemaName){
		this.table = table;
		this.column = field;
		this.schema = schemaName;
	}
	
	public String schemaTable(){
		if (schema == null)
			return table;
		return (schema + "." + table);
	}
	
	public String translationTableName() {
		 return table + "_" + column;
	}
	
	/**
	 * Returns the schema qualified name of the table in the translations 
	 * database where the mappings for this table field are stored.
	 * @return schema.table_column
	 */
	public String schemaQualifiedTranslationTableName() {
		return schema + "." + translationTableName();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((column == null) ? 0 : column.hashCode());
		result = prime * result
				+ ((schema == null) ? 0 : schema.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableField other = (TableField) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	@Override
	public String toString(){
		return table + "." + column;
	}
	
	public String toSQLString(){
		return "`" + table + "`.`" + column + "`";
	}
}
