import java.sql.*;
import java.util.Objects;
import java.util.StringJoiner;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;

/**
 * This is an example class how to create, connect and change Derby DB.
 * For more details @see <a href=http://db.apache.org/derby/papers/DerbyTut/index.html>Derby Tutorial</a>
 */

@SuppressWarnings({"CallToDriverManagerGetConnection",
        "NestedMethodCall", "UseOfSystemOutOrSystemErr",
        "CallToPrintStackTrace", "JDBCExecuteWithNonConstantString"})
public final class JavaDBTestClass {

    /**
     * If multiple applications needs to access the Derby database,
     * the Derby Network Server can be used
     * <p>
     * To use the Derby Network Server, set your CLASSPATH to include
     * derbynet.jar and derbytools.jar files from %JDK_HOME%\db\lib\
     * <p>
     * To start Derby Network Server execute command:
     * {@code $ java -jar %JDK_HOME%\db\lib\derbyrun.jar server start}
     * <p>
     * To stop Derby Network Server execute command:
     * {@code $ java -jar %JDK_HOME%\db\lib\derbyrun.jar server shutdown}
     * <p>
     * To use the Derby Network Client, set your CLASSPATH to include
     * derbyclient.jar and derbytools.jar files from %JDK_HOME%\db\lib\
     * <p>
     * Derby tries to create database within the %JAVA_HOME%\db\bin folder
     * by default. To create database in other folder, you need to specify the
     * absolute path in url
     */
    private static final String REMOTE_DB_PATH = "//localhost:1527/C:/temp/TestRemoteDB";
    /**
     * Only ONE application can connect to the Embedded Derby database at a time.
     * <p>
     * To use the Embedded Derby database, set your CLASSPATH to include
     * derby.jar and derbytools.jar files from %JDK_HOME%\db\lib\
     * <p>
     * But you don't need to start any more.
     */
    private static final String EMBEDDED_DB_PATH = "TestDB";
    private static final String REMOTE_CLIENT_DRIVER = "org.apache.derby.jdbc.ClientDriver";
    private static final String EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    /**
     * To create New DB you need to counstruct connection URL with create command:
     * {@code DriverManager.getConnection(jdbc:derby:{dbName};create=true);}
     * <p>
     * For further connections, you can use a simplified version:
     * {@code DriverManager.getConnection(jdbc:derby:{dbName});}
     */
    private static final String CREATE_DB_AND_CONNECT = "create=true";
    /**
     * A clean shutdown performs a checkpoint and releases resources.
     * If an embedded application doesn't shut down Derby,
     * a checkpoint won't be performed. Nothing bad will happen.
     * It just means that the next connection will be slower because Derby will run its recovery code.
     * <p>
     * To shut down a specific database:
     * {@code DriverManager.getConnection(jdbc:derby:{dbName};shutdown=true);}
     * <p>
     * To shut down all databases and the Derby engine:
     * {@code DriverManager.getConnection(jdbc:derby:;shutdown=true);}
     */
    private static final String SHUT_DOWN_DB = "shutdown=true";
    private static final String TABLE_ALREADY_EXISTS_STATE = "X0Y32";
    private static final String DUPLICATE_KEY_VALUE_STATE = "23505";
    private static final String INCORRECT_VALUE_TYPE_STATE = "42821";
    private static final String INCORRECT_NUMBER_OF_VALUES_STATE = "42802";
    private static final String createSpeciesTableQuery = "CREATE TABLE species (" +
            "id INTEGER PRIMARY KEY, " +
            "NAME VARCHAR(255), " +
            "num_acres DECIMAL(5,2))";
    private static final String createAnimalTableQuery = "CREATE TABLE animal (" +
            "id INTEGER PRIMARY KEY, " +
            "species_id INTEGER, " +
            "name VARCHAR(255), " +
            "date_born TIMESTAMP)";
    private static final String insertRowSpeciesQuery = "INSERT INTO species VALUES (%d, '%s', %f)";
    private static final String insertRowAnimalQuery = "INSERT INTO animal VALUES (%d, %d, '%s', '%s')";
    private static final String selectQuery = "SELECT * FROM %s";
    private static final String countRowsQuery = "SELECT count(*) as count FROM %s";
    private static final String url = String.format("jdbc:derby:%s;%s", EMBEDDED_DB_PATH, CREATE_DB_AND_CONNECT);

    public static void main(final String[] args) throws Exception {
        /*
         * Not required since JDBC 4.0 in Java 6.
         * META-INF/service/java.sql.Driver must be present inside the database .jar file.
         */
        Class.forName(EMBEDDED_DRIVER).getConstructor().newInstance();


        try (final Connection conn = DriverManager.getConnection(url);
             final Statement stmt = conn.createStatement()) {

            createTable(stmt, createSpeciesTableQuery);
            createTable(stmt, createAnimalTableQuery);

            insertRow(stmt, String.format(insertRowSpeciesQuery, 1, "African Elephant", 7.5));
            insertRow(stmt, String.format(insertRowSpeciesQuery, 2, "Zebra", 1.2));
            insertRow(stmt, String.format(insertRowAnimalQuery, 1, 1, "Elsa", "2001-05-06 02:15:00"));
            insertRow(stmt, String.format(insertRowAnimalQuery, 2, 2, "Zelda", "2002-08-15 09:12:00"));
            insertRow(stmt, String.format(insertRowAnimalQuery, 3, 1, "Ester", "2002-09-09 10:36:00"));
            insertRow(stmt, String.format(insertRowAnimalQuery, 4, 1, "Eddie", "2010-06-08 01:24:00"));
            insertRow(stmt, String.format(insertRowAnimalQuery, 5, 2, "Zoe", "2005-11-12 03:44:00"));

            final int speiesRowsNumber = countRows(stmt, String.format(countRowsQuery, "species"));
            System.out.printf("There are %d rows in the 'species' table", speiesRowsNumber);
            System.out.println();

            final int animalRowsNumber = countRows(stmt, String.format(countRowsQuery, "animal"));
            System.out.printf("There are %d rows in the 'animal' table", animalRowsNumber);
            System.out.println();

        } catch (final SQLException e) {
            e.printStackTrace();
        }

        System.out.printf("The last row in the 'species' table is: {%s}",
                getLastRow(String.format(selectQuery, "species")));
        System.out.println();
        System.out.printf("The last row in the 'animal' table is: {%s}",
                getLastRow(String.format(selectQuery, "animal")));
        System.out.println();
    }

    private static void createTable(final Statement stmt, final String query) throws SQLException {
        try {
            stmt.executeUpdate(query);
        } catch (final SQLException e) {
            if (Objects.equals(e.getSQLState(), TABLE_ALREADY_EXISTS_STATE)) {
                System.out.println(e.getMessage());
                return;
            }
            throw e;
        }
    }

    private static void insertRow(final Statement stmt, final String query) throws SQLException {
        try {
            stmt.executeUpdate(query);
        } catch (final SQLException e) {
            switch (e.getSQLState()) {
                case DUPLICATE_KEY_VALUE_STATE:
                    System.out.printf("The query '%s' was not executed because of duplicate key value.", query);
                    System.out.println();
                    break;
                case INCORRECT_VALUE_TYPE_STATE:
                    System.out.printf("The query '%s' was not executed. %s", query, e.getMessage());
                    System.out.println();
                    break;
                case INCORRECT_NUMBER_OF_VALUES_STATE:
                    System.out.printf("The query '%s' was not executed. %s", query, e.getMessage());
                    System.out.println();
                    break;
                default:
                    throw e;
            }
        }
    }

    private static int countRows(final Statement stmt, final String query) throws SQLException {
        try (final ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt("count");
            }
            return 0;
        }
    }

    private static String getLastRow(final String query) throws SQLException {
        try (final Connection conn = DriverManager.getConnection(url);
             final Statement stmt = conn.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY);
             final ResultSet rs = stmt.executeQuery(query)) {

            final StringJoiner sj = new StringJoiner(", ");
            rs.last();
            final ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                sj.add(rsmd.getColumnName(i) + "='" + rs.getString(i) + '\'');
            }
            return sj.toString();
        }
    }

}
