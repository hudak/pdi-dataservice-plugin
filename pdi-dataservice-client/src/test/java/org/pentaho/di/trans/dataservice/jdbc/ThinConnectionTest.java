package org.pentaho.di.trans.dataservice.jdbc;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Created by bmorrise on 9/28/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinConnectionTest extends JDBCTestBase {

  ThinConnection connection;
  Properties properties;

  @Before
  public void setUp() throws Exception {
    connection = spy( new ThinConnection.Builder().parseUrl( "jdbc:pdi://localhost:9080/kettle" ).build() );
    doReturn( "service return value" ).when( connection ).execService( anyString() );
    doReturn( mock( PostMethod.class ) ).when( connection )
      .execPost( anyString(), anyListOf( Header.class ), anyMapOf( String.class, Object.class ) );
    doReturn( "http://localhost/kettle" ).when( connection ).constructUrl( anyString() );

    properties = new Properties();
    properties.setProperty( "username", "username" );
    properties.setProperty( "password", "password" );
  }

  @Test
  public void testBuilder() throws Exception {
    String host = "localhost";
    String port = "9080";
    String webAppName = "pentaho-di";
    String proxyHostName = "proxyhostname";
    String proxyPort = "9081";
    String nonProxyHosts = "nonproxyhost";
    String debugTrans = "debugTrans";
    String debugLog = "true";
    String secure = "true";
    String local = "false";

    String
        url =
        "jdbc:pdi://" + host + ":" + port + "/kettle?webappname=" + webAppName + "&proxyhostname=" + proxyHostName
            + "&proxyport=" + proxyPort + "&nonproxyhosts=" + nonProxyHosts + "&debugtrans=" + debugTrans + "&debuglog="
            + debugLog + "&secure=" + secure + "&local=" + local;

    ThinConnection thinConnection = new ThinConnection.Builder().parseUrl( url ).readProperties( properties ).build();

    assertEquals( url, thinConnection.getUrl() );
    assertEquals( host, thinConnection.getHostname() );
    assertEquals( port, thinConnection.getPort() );
    assertEquals( webAppName, thinConnection.getWebAppName() );
    assertEquals( proxyHostName, thinConnection.getProxyHostname() );
    assertEquals( proxyPort, thinConnection.getProxyPort() );
    assertEquals( nonProxyHosts, thinConnection.getNonProxyHosts() );
    assertEquals( debugTrans, thinConnection.getDebugTransFilename() );
    assertEquals( true, thinConnection.isDebuggingRemoteLog() );
    assertEquals( true, thinConnection.isSecure() );
    assertEquals( false, thinConnection.isLocal() );
  }

  @Test
  public void testUnsupported() throws Exception {
    ImmutableSet<String> unsupportedFeatures = ImmutableSet.of(
      "commit", "createArrayOf", "createBlob", "createClob", "createNClob", "createSQLXML", "createStruct",
      "getHoldability", "getNetworkTimeout", "getTypeMap", "nativeSQL", "prepareCall", "releaseSavepoint", "rollback",
      "setHoldability", "setNetworkTimeout", "setSavepoint", "setTransactionIsolation",
      "setTypeMap"
    );

    verifyUnsupported( unsupportedFeatures, Connection.class, connection );
  }

  @Test
  public void testPrepareStatement() throws Exception {
    for ( Method method : Connection.class.getMethods() ) {
      if ( "prepareStatement".equals( method.getName() ) ) {
        assertThat( invoke( connection, method ), instanceOf( ThinPreparedStatement.class ) );
      }
    }

    verify( connection ).prepareStatement( anyString() );
  }

  @Test
  public void testCreateStatement() throws Exception {
    for ( Method method : Connection.class.getMethods() ) {
      if ( "createStatement".equals( method.getName() ) ) {
        assertThat( invoke( connection, method ), instanceOf( ThinStatement.class ) );
      }
    }

    verify( connection ).createStatement();
  }

  @Test
  public void testStatusCheck() throws Exception {
    connection.testConnection();
    verify( connection ).execService( "/kettle/status/" );

    assertThat( connection.isValid( 0 ), is( true ) );

    doThrow( new SQLException( new IOException() ) ).when( connection ).execService( anyString() );

    assertThat( connection.isValid( 0 ), is( false ) );
    try {
      connection.testConnection();
      fail( "Expected an exception" );
    } catch ( SQLException e ) {
      assertThat( e.getCause(), instanceOf( IOException.class ) );
    }

    connection.setLocal( true );
    try {
      connection.testConnection();
      fail( "Expected an exception" );
    } catch ( SQLException e ) {
      ThinConnection.localClient = mock( DataServiceClientService.class );
    }
    connection.testConnection();
    assertThat( connection.isValid( 0 ), is( true ) );
  }

  @Test
  public void testUnusedProperties() throws Exception {
    connection.setSchema( "schema " );
    assertThat( connection.getSchema(), nullValue() );

    connection.setCatalog( "catalog" );
    assertThat( connection.getCatalog(), nullValue() );

    connection.abort( mock( Executor.class ) );
    connection.close();
    assertThat( connection.isClosed(), is( false ) );

    connection.setAutoCommit( false );
    assertThat( connection.getAutoCommit(), is( true ) );

    connection.setReadOnly( false );
    assertThat( connection.isReadOnly(), is( true ) );

    connection.setClientInfo( properties );
    assertThat( connection.getClientInfo(), anEmptyMap() );

    assertThat( connection.getTransactionIsolation(), is( Connection.TRANSACTION_NONE ) );
  }
}
