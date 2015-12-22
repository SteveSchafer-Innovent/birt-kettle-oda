/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *
 *************************************************************************
 */
package innovent.kettle.oda.runtime.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.datatools.connectivity.oda.IBlob;
import org.eclipse.datatools.connectivity.oda.IClob;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;

/**
 * Implementation class of IResultSet for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded
 * implementation that returns a pre-defined set of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place.
 */
public class ResultSet implements IResultSet {
	private int maxRows;
	private int currentRowId;
	private final List<RowMetaAndData> rows;
	private final ResultSetMetaData meta;

	public ResultSet(final Result result) {
		this.rows = result.getRows();
		meta = new ResultSetMetaData(result);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getMetaData()
	 */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		return meta;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#setMaxRows(int)
	 */
	@Override
	public void setMaxRows(final int max) throws OdaException {
		maxRows = max;
	}

	/**
	 * Returns the maximum number of rows that can be fetched from this result
	 * set.
	 *
	 * @return the maximum number of rows to fetch.
	 */
	protected int getMaxRows() {
		return maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#next()
	 */
	@Override
	public boolean next() throws OdaException {
		int maxRows = getMaxRows();
		if (maxRows <= 0 || maxRows > rows.size()) // no limit is specified
			maxRows = rows.size();
		if (currentRowId < maxRows) {
			currentRowId++;
			return true;
		}
		return false;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#close()
	 */
	@Override
	public void close() throws OdaException {
		currentRowId = 0; // reset row counter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getRow()
	 */
	@Override
	public int getRow() throws OdaException {
		return currentRowId;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(int)
	 */
	@Override
	public String getString(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getString(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getString(java.lang
	 * .String)cu
	 */
	@Override
	public String getString(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			return row.getString(columnName, null);
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(int)
	 */
	@Override
	public int getInt(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getInt(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getInt(java.lang.String
	 * )
	 */
	@Override
	public int getInt(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final long value = row.getInteger(columnName, 0);
			return Long.valueOf(value).intValue();
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(int)
	 */
	@Override
	public double getDouble(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getInt(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(java.lang
	 * .String)
	 */
	@Override
	public double getDouble(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final double value = row.getNumber(columnName, 0.0);
			return Double.valueOf(value).doubleValue();
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(int)
	 */
	@Override
	public BigDecimal getBigDecimal(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getBigDecimal(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(java.
	 * lang.String)
	 */
	@Override
	public BigDecimal getBigDecimal(final String columnName)
			throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final BigDecimal value = row.getBigNumber(columnName,
					BigDecimal.valueOf(0.0));
			return value;
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(int)
	 */
	@Override
	public Date getDate(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getDate(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getDate(java.lang.String
	 * )
	 */
	@Override
	public Date getDate(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final java.util.Date value = row.getDate(columnName, null);
			if (value == null)
				return null;
			return new java.sql.Date(value.getTime());
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(int)
	 */
	@Override
	public Time getTime(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getTime(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getTime(java.lang.String
	 * )
	 */
	@Override
	public Time getTime(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final java.util.Date value = row.getDate(columnName, null);
			if (value == null)
				return null;
			return new java.sql.Time(value.getTime());
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(int)
	 */
	@Override
	public Timestamp getTimestamp(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getTimestamp(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(java.lang
	 * .String)
	 */
	@Override
	public Timestamp getTimestamp(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final java.util.Date value = row.getDate(columnName, null);
			if (value == null)
				return null;
			return new java.sql.Timestamp(value.getTime());
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(int)
	 */
	@Override
	public IBlob getBlob(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getBlob(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(java.lang.String
	 * )
	 */
	@Override
	public IBlob getBlob(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final byte[] value = row.getBinary(columnName, null);
			if (value == null)
				return null;
			return new IBlob() {
				@Override
				public InputStream getBinaryStream() throws OdaException {
					return new ByteArrayInputStream(value);
				}

				@Override
				public byte[] getBytes(final long arg0, final int arg1)
						throws OdaException {
					final List<Byte> list = new ArrayList<>();
					for (int i = Long.valueOf(arg0).intValue(); i < arg0 + arg1; i++) {
						list.add(Byte.valueOf(value[i]));
					}
					final byte[] result = new byte[list.size()];
					int i = 0;
					for (final Byte b : list) {
						result[i++] = b;
					}
					return result;
				}

				@Override
				public long length() throws OdaException {
					return value.length;
				}
			};
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(int)
	 */
	@Override
	public IClob getClob(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getClob(columnName);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getClob(java.lang.String
	 * )
	 */
	@Override
	public IClob getClob(final String columnName) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(int)
	 */
	@Override
	public boolean getBoolean(final int index) throws OdaException {
		final String columnName = meta.getColumnName(index);
		return getBoolean(columnName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(java.lang
	 * .String)
	 */
	@Override
	public boolean getBoolean(final String columnName) throws OdaException {
		final RowMetaAndData row = rows.get(currentRowId - 1);
		try {
			final boolean value = row.getBoolean(columnName, false);
			return value;
		}
		catch (final KettleValueException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(int)
	 */
	@Override
	public Object getObject(final int index) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getObject(java.lang
	 * .String)
	 */
	@Override
	public Object getObject(final String columnName) throws OdaException {
		throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#wasNull()
	 */
	@Override
	public boolean wasNull() throws OdaException {
		return false;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#findColumn(java.lang
	 * .String)
	 */
	@Override
	public int findColumn(final String columnName) throws OdaException {
		final Integer result = meta.fieldNameMapping.get(columnName);
		if (result == null) {
			throw new OdaException("Unknown column name: " + columnName);
		}
		return result.intValue();
	}
}
