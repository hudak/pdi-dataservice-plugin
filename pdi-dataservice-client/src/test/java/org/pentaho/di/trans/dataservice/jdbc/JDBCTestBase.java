package org.pentaho.di.trans.dataservice.jdbc;

import com.google.common.collect.Sets;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author nhudak
 */
public abstract class JDBCTestBase {
  protected void verifyUnsupported( Set<String> unsupported, Class<Connection> type, Object object ) {
    Set<String> invocations = Sets.newHashSet();
    for ( Method method : type.getMethods() ) {
      if ( unsupported.contains( method.getName() ) ) {
        invocations.add( method.getName() );
        try {
          invoke( object, method );
          fail( "Expected SQLFeatureNotSupportedException from " + method );
        } catch ( InvocationTargetException e ) {
          assertThat( method.getName(), e.getCause(), instanceOf( SQLFeatureNotSupportedException.class ) );
        } catch ( Exception e ) {
          throw new AssertionError( "Error executing " + method, e );
        }
      }
    }

    assertThat( "Unsupported methods invoked ", invocations, equalTo( unsupported ) );
  }

  protected void verifyNoOp( Set<String> noOps, Class<Connection> type, Object object ) throws Exception {
    Set<String> invocations = Sets.newHashSet();
    for ( Method method : type.getMethods() ) {
      if ( noOps.contains( method.getName() ) ) {
        assertThat( method.toString(), invoke( object, method ), nullValue() );
        invocations.add( method.getName() );
      }
    }

    assertThat( "No-op methods invoked ", invocations, equalTo( noOps ) );
  }

  protected Object invoke( Object object, Method method ) throws IllegalAccessException, InvocationTargetException {
    return method.invoke( object, mockArguments( method ) );
  }

  protected Object[] mockArguments( Method method ) {
    Class<?>[] parameterTypes = method.getParameterTypes();
    Object[] args = new Object[parameterTypes.length];
    for ( int i = 0; i < parameterTypes.length; i++ ) {
      args[i] = mockValue( parameterTypes[i] );
    }
    return args;
  }

  protected Object mockValue( Class<?> type ) {
    Object value;
    if ( type.equals( String.class ) ) {
      value = UUID.randomUUID().toString();
    } else if ( type.equals( Boolean.TYPE ) ) {
      value = true;
    } else if ( type.equals( Integer.TYPE ) ) {
      value = new Random().nextInt();
    } else if ( type.equals( Class.class ) ) {
      value = Object.class;
    } else if ( type.isArray() ) {
      value = Array.newInstance( type.getComponentType(), 0 );
    } else {
      value = mock( type );
    }
    return value;
  }
}
