package org.datapipeline;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DatabaseManager  {

        // not final anymore and null as default
        private static DatabaseManager instance = null;

        public DatabaseManager() throws PropertyVetoException {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
            dataSource.setJdbcUrl("jdbc:mysql:///cap5000?cloudSqlInstance=dkt-us-data-lake-a1xq:us-west2:mulesoftdbinstance-staging&socketFactory=com.google.cloud.sql.mysql.SocketFactory");
            dataSource.setUser("cap5000");
            dataSource.setPassword("Mobilitech/20");

            // use a try-with resource to get rid of the finally block...
            try{
                JdbcIO.DataSourceConfiguration.create(dataSource);
            } catch(Exception e){
                System.err.println("SQLException while constructing the instance of DatabaseManager");
            }
        }


    }

