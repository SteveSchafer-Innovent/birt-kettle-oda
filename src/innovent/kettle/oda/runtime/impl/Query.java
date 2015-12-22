/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *
 *************************************************************************
 */
package innovent.kettle.oda.runtime.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

/**
 * Implementation class of IQuery for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded
 * implementation that returns a pre-defined set of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place.
 */
public class Query implements IQuery {
	private int m_maxRows;
	private String preparedText = null;
	private JobMeta jobMeta = null;

	public Query() {
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#prepare(java.lang.String)
	 */
	@Override
	public void prepare(final String queryText) throws OdaException {
		try {
			jobMeta = new JobMeta(queryText, null);
			preparedText = queryText;
		}
		catch (final KettleXMLException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setAppContext(java.lang
	 * .Object)
	 */
	@Override
	public void setAppContext(final Object context) throws OdaException {
		// do nothing; assumes no support for pass-through context
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#close()
	 */
	@Override
	public void close() throws OdaException {
		preparedText = null;
		jobMeta = null;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMetaData()
	 */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		final Job job = new Job(null, jobMeta);
		job.setLogLevel(LogLevel.MINIMAL);
		job.start();
		job.waitUntilFinished();
		final Result result = job.getResult();
		return new ResultSetMetaData(result);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#executeQuery()
	 */
	@Override
	public IResultSet executeQuery() throws OdaException {
		final Job job = new Job(null, jobMeta);
		job.setLogLevel(LogLevel.MINIMAL);
		job.start();
		job.waitUntilFinished();
		final Result result = job.getResult();
		final IResultSet resultSet = new ResultSet(result);
		resultSet.setMaxRows(getMaxRows());
		return resultSet;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setProperty(java.lang.String
	 * , java.lang.String)
	 */
	@Override
	public void setProperty(final String name, final String value)
			throws OdaException {
		// do nothing; assumes no data set query property
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setMaxRows(int)
	 */
	@Override
	public void setMaxRows(final int max) throws OdaException {
		m_maxRows = max;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMaxRows()
	 */
	@Override
	public int getMaxRows() throws OdaException {
		return m_maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#clearInParameters()
	 */
	@Override
	public void clearInParameters() throws OdaException {
		jobMeta.clearParameters();
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setInt(java.lang.String,
	 * int)
	 */
	@Override
	public void setInt(final String parameterName, final int value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, String.valueOf(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(int, int)
	 */
	@Override
	public void setInt(final int parameterId, final int value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setInt(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setDouble(java.lang.String,
	 * double)
	 */
	@Override
	public void setDouble(final String parameterName, final double value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, String.valueOf(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(int, double)
	 */
	@Override
	public void setDouble(final int parameterId, final double value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setDouble(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(java.lang
	 * .String, java.math.BigDecimal)
	 */
	@Override
	public void setBigDecimal(final String parameterName, final BigDecimal value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, String.valueOf(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(int,
	 * java.math.BigDecimal)
	 */
	@Override
	public void setBigDecimal(final int parameterId, final BigDecimal value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setBigDecimal(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setString(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void setString(final String parameterName, final String value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, value);
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(int,
	 * java.lang.String)
	 */
	@Override
	public void setString(final int parameterId, final String value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setString(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setDate(java.lang.String,
	 * java.sql.Date)
	 */
	@Override
	public void setDate(final String parameterName, final Date value)
			throws OdaException {
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			jobMeta.setParameterValue(parameterName, df.format(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(int,
	 * java.sql.Date)
	 */
	@Override
	public void setDate(final int parameterId, final Date value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setDate(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setTime(java.lang.String,
	 * java.sql.Time)
	 */
	@Override
	public void setTime(final String parameterName, final Time value)
			throws OdaException {
		final DateFormat df = new SimpleDateFormat("HH:mm:ss");
		try {
			jobMeta.setParameterValue(parameterName, df.format(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(int,
	 * java.sql.Time)
	 */
	@Override
	public void setTime(final int parameterId, final Time value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setTime(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(java.lang.
	 * String, java.sql.Timestamp)
	 */
	@Override
	public void setTimestamp(final String parameterName, final Timestamp value)
			throws OdaException {
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			jobMeta.setParameterValue(parameterName, df.format(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(int,
	 * java.sql.Timestamp)
	 */
	@Override
	public void setTimestamp(final int parameterId, final Timestamp value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setTimestamp(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(java.lang.String
	 * , boolean)
	 */
	@Override
	public void setBoolean(final String parameterName, final boolean value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, String.valueOf(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(int,
	 * boolean)
	 */
	@Override
	public void setBoolean(final int parameterId, final boolean value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setBoolean(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setObject(java.lang.String,
	 * java.lang.Object)
	 */
	@Override
	public void setObject(final String parameterName, final Object value)
			throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, String.valueOf(value));
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(int,
	 * java.lang.Object)
	 */
	@Override
	public void setObject(final int parameterId, final Object value)
			throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setObject(parameters[parameterId], value);
		// only applies to input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setNull(java.lang.String)
	 */
	@Override
	public void setNull(final String parameterName) throws OdaException {
		try {
			jobMeta.setParameterValue(parameterName, null);
		}
		catch (final UnknownParamException e) {
			throw new OdaException(e);
		}
		// only applies to named input parameter
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(int)
	 */
	@Override
	public void setNull(final int parameterId) throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		setNull(parameters[parameterId]);
		// only applies to input parameter
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#findInParameter(java.lang
	 * .String)
	 */
	@Override
	public int findInParameter(final String parameterName) throws OdaException {
		final String[] parameters = jobMeta.listParameters();
		for (int i = 0; i < parameters.length; i++) {
			if (parameterName == null ? parameters[i] == null : parameterName
					.equals(parameters[i]))
				return i;
		}
		// only applies to named input parameter
		throw new OdaException("Parameter not found");
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getParameterMetaData()
	 */
	@Override
	public IParameterMetaData getParameterMetaData() throws OdaException {
		return new ParameterMetaData(jobMeta);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setSortSpec(org.eclipse
	 * .datatools.connectivity.oda.SortSpec)
	 */
	@Override
	public void setSortSpec(final SortSpec sortBy) throws OdaException {
		// only applies to sorting, assumes not supported
		throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getSortSpec()
	 */
	@Override
	public SortSpec getSortSpec() throws OdaException {
		// only applies to sorting
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#setSpecification(org.eclipse
	 * .datatools.connectivity.oda.spec.QuerySpecification)
	 */
	@Override
	public void setSpecification(final QuerySpecification querySpec)
			throws OdaException, UnsupportedOperationException {
		// assumes no support
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getSpecification()
	 */
	@Override
	public QuerySpecification getSpecification() {
		// assumes no support
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IQuery#getEffectiveQueryText()
	 */
	@Override
	public String getEffectiveQueryText() {
		// TODO Auto-generated method stub
		return preparedText;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#cancel()
	 */
	@Override
	public void cancel() throws OdaException, UnsupportedOperationException {
		// assumes unable to cancel while executing a query
		throw new UnsupportedOperationException();
	}
}
