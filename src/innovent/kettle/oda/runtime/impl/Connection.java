/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *
 *************************************************************************
 */
package innovent.kettle.oda.runtime.impl;

import java.util.Properties;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;

import com.ibm.icu.util.ULocale;

/**
 * Implementation class of IConnection for an ODA runtime driver.
 */
public class Connection implements IConnection {
	private static boolean kettleStarted = false;
	private boolean open = false;

	private void startKettle() {
		if (!kettleStarted) {
			try {
				KettleEnvironment.init();
			}
			catch (final KettleException e) {
				throw new RuntimeException(
						"Unable to initialize the Kettle environment", e);
			}
			kettleStarted = true;
		}
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IConnection#open(java.util.Properties
	 * )
	 */
	@Override
	public void open(final Properties connProperties) throws OdaException {
		startKettle();
		open = true;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IConnection#setAppContext(java
	 * .lang.Object)
	 */
	@Override
	public void setAppContext(final Object context) throws OdaException {
		// do nothing; assumes no support for pass-through context
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#close()
	 */
	@Override
	public void close() throws OdaException {
		open = false;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#isOpen()
	 */
	@Override
	public boolean isOpen() throws OdaException {
		return open;
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IConnection#getMetaData(java.lang
	 * .String)
	 */
	@Override
	public IDataSetMetaData getMetaData(final String dataSetType)
			throws OdaException {
		// assumes that this driver supports only one type of data set,
		// ignores the specified dataSetType
		return new DataSetMetaData(this);
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IConnection#newQuery(java.lang
	 * .String)
	 */
	@Override
	public IQuery newQuery(final String dataSetType) throws OdaException {
		// assumes that this driver supports only one type of data set,
		// ignores the specified dataSetType
		return new Query();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#getMaxQueries()
	 */
	@Override
	public int getMaxQueries() throws OdaException {
		return 0; // no limit
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#commit()
	 */
	@Override
	public void commit() throws OdaException {
		// do nothing; assumes no transaction support needed
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#rollback()
	 */
	@Override
	public void rollback() throws OdaException {
		// do nothing; assumes no transaction support needed
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IConnection#setLocale(com.ibm.
	 * icu.util.ULocale)
	 */
	@Override
	public void setLocale(final ULocale locale) throws OdaException {
		// do nothing; assumes no locale support
	}
}
