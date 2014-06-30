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


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Wraps a java.sql.ResultSet to only allow retrieval of column values.
 *
 */
public class ResultSetRowReader {
	ResultSet resultSet;
	
	public ResultSetRowReader(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public Object getObject(int column) throws SQLException {
		return resultSet.getObject(column);
	}
	
	public Object getObject(String column) throws SQLException {
		return resultSet.getObject(column);
	}

	public String getString(int column) throws SQLException {
		return resultSet.getString(column);
	}
	
	public String getString(String column) throws SQLException {
		return resultSet.getString(column);
	}

	public int getInt(int column) throws SQLException {
		return resultSet.getInt(column);
	}
	
	public int getInt(String column) throws SQLException {
		return resultSet.getInt(column);
	}
	
	public Array getArray(int column) throws SQLException {
		return resultSet.getArray(column);
	}
	
	public Array getArray(String column) throws SQLException {
		return resultSet.getArray(column);
	}

	public InputStream getAsciiStream(int column) throws SQLException {
		return resultSet.getAsciiStream(column);
	}
	
	public InputStream getAsciiStream(String column) throws SQLException {
		return resultSet.getAsciiStream(column);
	}

	public BigDecimal getBigDecimal(int column) throws SQLException {
		return resultSet.getBigDecimal(column);
	}
	
	public BigDecimal getBigDecimal(String column) throws SQLException {
		return resultSet.getBigDecimal(column);
	}

	public InputStream getBinaryStream(int column) throws SQLException {
		return resultSet.getBinaryStream(column);
	}
	
	public InputStream getBinaryStream(String column) throws SQLException {
		return resultSet.getBinaryStream(column);
	}

	public Blob getBlob(int column) throws SQLException {
		return resultSet.getBlob(column);
	}
	
	public Blob getBlob(String column) throws SQLException {
		return resultSet.getBlob(column);
	}

	public boolean getBoolean(int column) throws SQLException {
		return resultSet.getBoolean(column);
	}
	
	public boolean getBoolean(String column) throws SQLException {
		return resultSet.getBoolean(column);
	}

	public byte getByte(int column) throws SQLException {
		return resultSet.getByte(column);
	}
	
	public byte getByte(String column) throws SQLException {
		return resultSet.getByte(column);
	}

	public byte[] getBytes(int column) throws SQLException {
		return resultSet.getBytes(column);
	}
	
	public byte[] getBytes(String column) throws SQLException {
		return resultSet.getBytes(column);
	}

	public Reader getCharacterStream(int column) throws SQLException {
		return resultSet.getCharacterStream(column);
	}
	
	public Reader getCharacterStream(String column) throws SQLException {
		return resultSet.getCharacterStream(column);
	}

	public Clob getClob(int column) throws SQLException {
		return resultSet.getClob(column);
	}
	
	public Clob getClob(String column) throws SQLException {
		return resultSet.getClob(column);
	}

	public Date getDate(int column) throws SQLException {
		return resultSet.getDate(column);
	}
	
	public Date getDate(String column) throws SQLException {
		return resultSet.getDate(column);
	}

	public double getDouble(int column) throws SQLException {
		return resultSet.getDouble(column);
	}
	
	public double getDouble(String column) throws SQLException {
		return resultSet.getDouble(column);
	}

	public float getFloat(int column) throws SQLException {
		return resultSet.getFloat(column);
	}
	
	public float getFloat(String column) throws SQLException {
		return resultSet.getFloat(column);
	}

	public long getLong(int column) throws SQLException {
		return resultSet.getLong(column);
	}
	
	public long getLong(String column) throws SQLException {
		return resultSet.getLong(column);
	}

	public Reader getNCharacterStream(int column) throws SQLException {
		return resultSet.getNCharacterStream(column);
	}
	
	public Reader getNCharacterStream(String column) throws SQLException {
		return resultSet.getNCharacterStream(column);
	}

	public NClob getNClob(int column) throws SQLException {
		return resultSet.getNClob(column);
	}
	
	public NClob getNClob(String column) throws SQLException {
		return resultSet.getNClob(column);
	}

	public String getNString(int column) throws SQLException {
		return resultSet.getNString(column);
	}
	
	public String getNString(String column) throws SQLException {
		return resultSet.getNString(column);
	}

	public Ref getRef(int column) throws SQLException {
		return resultSet.getRef(column);
	}
	
	public Ref getRef(String column) throws SQLException {
		return resultSet.getRef(column);
	}

	public RowId getRowId(int column) throws SQLException {
		return resultSet.getRowId(column);
	}
	
	public RowId getRowId(String column) throws SQLException {
		return resultSet.getRowId(column);
	}

	public short getShort(int column) throws SQLException {
		return resultSet.getShort(column);
	}
	
	public short getShort(String column) throws SQLException {
		return resultSet.getShort(column);
	}

	public SQLXML getSQLXML(int column) throws SQLException {
		return resultSet.getSQLXML(column);
	}
	
	public SQLXML getSQLXML(String column) throws SQLException {
		return resultSet.getSQLXML(column);
	}

	public Time getTime(int column) throws SQLException {
		return resultSet.getTime(column);
	}
	
	public Time getTime(String column) throws SQLException {
		return resultSet.getTime(column);
	}

	public Timestamp getTimestamp(int column) throws SQLException {
		return resultSet.getTimestamp(column);
	}
	
	public Timestamp getTimestamp(String column) throws SQLException {
		return resultSet.getTimestamp(column);
	}

	public URL getURL(int column) throws SQLException {
		return resultSet.getURL(column);
	}
	
	public URL getURL(String column) throws SQLException {
		return resultSet.getURL(column);
	}
}
