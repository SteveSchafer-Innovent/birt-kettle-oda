/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *
 *************************************************************************
 */
package innovent.kettle.oda.runtime.impl;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.pentaho.di.job.JobMeta;

/**
 * Implementation class of IParameterMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded
 * implementation that returns a pre-defined set of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place.
 */
public class ParameterMetaData implements IParameterMetaData {
	private final JobMeta jobMeta;

	public ParameterMetaData(final JobMeta jobMeta) {
		this.jobMeta = jobMeta;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getParameterCount
	 * ()
	 */
	@Override
	public int getParameterCount() throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		return parameters.length;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getParameterMode
	 * (int)
	 */
	@Override
	public int getParameterMode(final int param) throws OdaException {
		return IParameterMetaData.parameterModeIn;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getParameterName
	 * (int)
	 */
	@Override
	public String getParameterName(final int param) throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		return param < 0 || param >= parameters.length ? null
				: parameters[param];
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getParameterType
	 * (int)
	 */
	@Override
	public int getParameterType(final int param) throws OdaException {
		return java.sql.Types.VARCHAR;
		// as defined in data set extension manifest
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IParameterMetaData#
	 * getParameterTypeName(int)
	 */
	@Override
	public String getParameterTypeName(final int param) throws OdaException {
		final int nativeTypeCode = getParameterType(param);
		return Driver.getNativeDataTypeName(nativeTypeCode);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getPrecision
	 * (int)
	 */
	@Override
	public int getPrecision(final int param) throws OdaException {
		return -1;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#getScale(int)
	 */
	@Override
	public int getScale(final int param) throws OdaException {
		return -1;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IParameterMetaData#isNullable(int)
	 */
	@Override
	public int isNullable(final int param) throws OdaException {
		return IParameterMetaData.parameterNullableUnknown;
	}
}
