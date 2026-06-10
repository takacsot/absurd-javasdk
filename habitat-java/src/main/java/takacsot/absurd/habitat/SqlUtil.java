package takacsot.absurd.habitat;

public final class SqlUtil {

    private SqlUtil() {}

    public static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    public static String queueTable(String prefix, String queueName) {
        return quoteIdentifier(prefix + "_" + queueName);
    }
}
