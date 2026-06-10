package takacsot.absurd.habitat;

public class HabitatConfig {

    private String listenAddress = ":7899";
    private String basePath = "";
    private String dbUrl = "";
    private String dbHost = "localhost";
    private int dbPort = 5432;
    private String dbUser = "";
    private String dbPassword = "";
    private String dbName = "absurd";
    private String dbSslMode = "disable";

    private String templateDir = "";

    public static HabitatConfig fromEnv() {
        HabitatConfig cfg = new HabitatConfig();
        cfg.listenAddress = env("HABITAT_LISTEN", cfg.listenAddress);
        cfg.basePath = env("HABITAT_BASE_PATH", cfg.basePath);
        cfg.dbUrl = env("HABITAT_DB_URL", cfg.dbUrl);
        cfg.dbHost = env("HABITAT_DB_HOST", cfg.dbHost);
        cfg.dbPort = envInt("HABITAT_DB_PORT", cfg.dbPort);
        cfg.dbUser = env("HABITAT_DB_USER", cfg.dbUser);
        cfg.dbPassword = env("HABITAT_DB_PASSWORD", cfg.dbPassword);
        cfg.dbName = env("HABITAT_DB_NAME", "takacso" /*cfg.dbName*/);
        cfg.dbSslMode = env("HABITAT_DB_SSLMODE", cfg.dbSslMode);
        cfg.templateDir = env("HABITAT_TEMPLATE_DIR", "/Users/takacso/Resources/absurd/absurd-java/src/main/resources/templates" /*cfg.templateDir*/);
        return cfg;
    }

    public String jdbcUrl() {
        if (!dbUrl.isEmpty()) {
            return dbUrl;
        }
        return "jdbc:postgresql://%s:%d/%s?sslmode=%s".formatted(dbHost, dbPort, dbName, dbSslMode);
    }

    public int port() {
        String addr = listenAddress;
        int idx = addr.lastIndexOf(':');
        if (idx >= 0) {
            try {
                return Integer.parseInt(addr.substring(idx + 1));
            } catch (NumberFormatException e) {
                return 7890;
            }
        }
        return 7890;
    }

    public String basePath() { return basePath; }
    public String dbUser() { return dbUser; }
    public String dbPassword() { return dbPassword; }
    public String templateDir() { return templateDir; }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return (v != null && !v.isEmpty()) ? v : fallback;
    }

    private static int envInt(String name, int fallback) {
        String v = System.getenv(name);
        if (v != null && !v.isEmpty()) {
            try { return Integer.parseInt(v); } catch (NumberFormatException e) { /* ignore */ }
        }
        return fallback;
    }
}
