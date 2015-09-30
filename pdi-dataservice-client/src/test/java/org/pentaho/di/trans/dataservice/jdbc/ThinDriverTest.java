/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.jdbc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ThinDriverTest {

  public static final String URL = "jdbc:pdi://slaveserver:8181/kettle/?webappname=pdi";
  private ThinDriver driver;
  private Properties properties;

  @Mock ThinConnection connection;

  @Before
  public void setUp() throws Exception {
    properties = new Properties();
    properties.setProperty( "user", "joe" );
    properties.setProperty( "password", "drowssap" );

    driver = new ThinDriver() {
      @Override protected ThinConnection createConnection( String url, Properties properties ) throws SQLException {
        assertThat( url, is( URL ) );
        assertThat( properties, equalTo( ThinDriverTest.this.properties ) );

        // Ensure no exceptions thrown by real method
        super.createConnection( url, properties );

        // Inject mock
        return connection;
      }
    };
  }

  @Test
  public void testAcceptsURL() throws Exception {
    assertTrue( driver.acceptsURL( URL ) );
    assertFalse( driver.acceptsURL( "jdbc:mysql://localhost" ) );
  }

  @Test
  public void testConnectNull() throws Exception {
    assertNull( driver.connect( "jdbc:mysql://localhost", properties ) );
    verify( connection, never() ).testConnection();
  }

  @Test
  public void testDriverProperties() throws Exception {
    assertThat( driver.getMajorVersion(), greaterThan( 0 ) );
    assertThat( driver.getMinorVersion(), greaterThanOrEqualTo( 0 ) );
    assertThat( driver.getPropertyInfo( URL, properties ), emptyArray() );
    assertThat( driver.jdbcCompliant(), is( false ) );
    assertThat( driver.getParentLogger(), notNullValue() );
  }

  @Test
  public void testConnectValid() throws Exception {
    when( connection.testConnection() ).thenReturn( connection );
    assertThat( driver.connect( URL, properties ), sameInstance( (Connection) connection ) );
    verify( connection ).testConnection();
  }

  @Test
  public void testConnectError() throws SQLException {
    final SQLException expected = new SQLException( "Test Connection Error" );
    doThrow( expected ).when( connection ).testConnection();

    try {
      driver.connect( URL, properties );
      fail( "Unexpected connection" );
    } catch ( SQLException e ) {
      // Expected exception
      assertThat( e, sameInstance( expected ) );
    }
  }
}
