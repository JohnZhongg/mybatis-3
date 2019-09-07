/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.logging.jdbc;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionLoggerTest {

  @Mock
  Connection connection;

  @Mock
  PreparedStatement preparedStatement;

  @Mock
  Log log;

  Connection conn;

  @Before
  public void setUp() throws SQLException {
    when(log.isDebugEnabled()).thenReturn(true);
    conn = ConnectionLogger.newInstance(connection, log, 1);
  }

  @Test
  public void shouldPrintPrepareStatement() throws SQLException {
    conn.prepareStatement("select 1");
    verify(log).debug(contains("Preparing: select 1"));
  }

  @Test
  public void shouldPrintPrepareCall() throws SQLException {
    conn.prepareCall("{ call test() }");
    verify(log).debug(contains("Preparing: { call test() }"));
  }

  @Test
  public void shouldNotPrintCreateStatement() throws SQLException {
    conn.createStatement();
    conn.close();
    verify(log, times(0)).debug(anyString());
  }

  @Test
  public void myTest() throws SQLException, ClassNotFoundException {
    LogFactory.useStdOutLogging();
    Log log = LogFactory.getLog(this.getClass());
    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:mysql://10.211.55.128:3306/vicutu", "root", "root");
    connection = ConnectionLogger.newInstance(connection, log, 2);
    PreparedStatement preparedStatement = connection.prepareStatement("select * from it_dir");
    ResultSet resultSet = preparedStatement.executeQuery();
    while (resultSet.next()) {
      long aLong = resultSet.getLong(1);
    }

  }


  @Test
  public void myTest1() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:mysql://10.211.55.128:3306", "root", "root");
    PreparedStatement preparedStatement = connection.prepareStatement("");
    ResultSet resultSet = preparedStatement.executeQuery("select 1");
    resultSet.next();
    int anInt = resultSet.getInt(1);
    System.out.println(anInt);


  }
}
