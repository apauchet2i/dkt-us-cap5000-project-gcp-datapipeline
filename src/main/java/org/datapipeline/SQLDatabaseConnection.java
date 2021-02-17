package org.datapipeline;

import java.sql.*;

public class SQLDatabaseConnection(beam.Dfn) {

        // Connect to your database.
        // Replace server name, username, and password with your credentials
        public static void main(String[] args) {
            String connectionUrl =
                    "jdbc:sqlserver://yourserver.database.windows.net:1433;"
                            + "database=AdventureWorks;"
                            + "user=yourusername@yourserver;"
                            + "password=yourpassword;"
                            + "encrypt=true;"
                            + "trustServerCertificate=false;"
                            + "loginTimeout=30;";

            ResultSet resultSet = null;
            try (Connection connection = DriverManager.getConnection(connectionUrl);) {
                Statement statement = connection.createStatement();

                    // Create and execute a SELECT SQL statement.
                    String selectSql = "SELECT TOP 10 Title, FirstName, LastName from SalesLT.Customer";
                    resultSet = statement.executeQuery(selectSql);

                }
            // Handle any errors that may have occurred.
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
