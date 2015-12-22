/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *
 *************************************************************************
 */
package innovent.kettle.oda.runtime.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMetaInterface;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded
 * implementation that returns a pre-defined set of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place.
 */
public class ResultSetMetaData implements IResultSetMetaData {
	final List<String> fieldNames = new ArrayList<>();
	final Map<String, Integer> fieldNameMapping = new HashMap<>();

	public ResultSetMetaData(final Result result) {
		final Set<String> fieldNameSet = new HashSet<>();
		final List<RowMetaAndData> rows = result.getRows();
		for (final RowMetaAndData rmad : rows) {
			final RowMetaInterface rowMeta = rmad.getRowMeta();
			final String[] fieldNames = rowMeta.getFieldNames();
			for (final String fieldName : fieldNames) {
				fieldNameSet.add(fieldName);
			}
		}
		this.fieldNames.addAll(fieldNameSet);
		Collections.sort(fieldNames);
		int i = 0;
		for (final String fieldName : fieldNames) {
			fieldNameMapping.put(fieldName, Integer.valueOf(i++));
		}
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnCount
	 * ()
	 */
	@Override
	public int getColumnCount() throws OdaException {
		return fieldNames.size();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnName
	 * (int)
	 */
	@Override
	public String getColumnName(final int index) throws OdaException {
		if (index < 1 || index > fieldNames.size()) {
			throw new OdaException(index + " is not a valid column index");
		}
		return fieldNames.get(index - 1);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnLabel
	 * (int)
	 */
	@Override
	public String getColumnLabel(final int index) throws OdaException {
		return getColumnName(index); // default
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnType
	 * (int)
	 */
	@Override
	public int getColumnType(final int index) throws OdaException {
		return java.sql.Types.VARCHAR;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnTypeName
	 * (int)
	 */
	@Override
	public String getColumnTypeName(final int index) throws OdaException {
		final int nativeTypeCode = getColumnType(index);
		return Driver.getNativeDataTypeName(nativeTypeCode);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#
	 * getColumnDisplayLength(int)
	 */
	@Override
	public int getColumnDisplayLength(final int index) throws OdaException {
		return 8;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getPrecision
	 * (int)
	 */
	@Override
	public int getPrecision(final int index) throws OdaException {
		return -1;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getScale(int)
	 */
	@Override
	public int getScale(final int index) throws OdaException {
		return -1;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSetMetaData#isNullable(int)
	 */
	@Override
	public int isNullable(final int index) throws OdaException {
		return IResultSetMetaData.columnNullableUnknown;
	}
}
