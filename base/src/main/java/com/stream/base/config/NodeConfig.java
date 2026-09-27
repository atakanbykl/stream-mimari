package com.stream.base.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * node-* servisleri için dış dosya (external file) tabanlı, derleme gerektirmeyen
 * ve çalışma zamanında katı doğrulama (strict validation) yapan yapılandırma yöneticisi.
 */
public class NodeConfig {

    public static final String NATS_URL = "nats.url";
    public static final String NATS_CONNECTION_NAME = "nats.connection.name";
    public static final String NATS_RECONNECT_WAIT_SEC = "nats.reconnect.wait.seconds";
    public static final String NATS_MAX_RECONNECTS = "nats.max.reconnects";

    public static final String NATS_STREAM_NAME = "nats.stream.name";
    public static final String NATS_STREAM_SUBJECTS = "nats.stream.subjects";
    public static final String NATS_STREAM_STORAGE = "nats.stream.storage";
    public static final String NATS_STREAM_ALLOW_DIRECT = "nats.stream.allow.direct";
    public static final String NATS_STREAM_REPLICAS = "nats.stream.replicas";

    private final String nodeId;
    private final Properties properties = new Properties();
    private boolean externalFileLoaded = false;
    private String loadedSource = "YOK (DOSYA BULUNAMADI)";
    private final List<String> searchedLocations = new ArrayList<>();
    private final List<String> validationErrors = new ArrayList<>();

    public NodeConfig(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Belirtilen node kimliği için dış yapılandırma dosyasını yükler.
     */
    public static NodeConfig load(String nodeId) {
        NodeConfig config = new NodeConfig(nodeId);
        config.reload();
        return config;
    }

    /**
     * Birim testleri veya mock ortamlar için doğrudan Properties nesnesinden üretir.
     */
    public static NodeConfig fromProperties(String nodeId, Properties props) {
        NodeConfig config = new NodeConfig(nodeId);
        if (props != null) {
            config.properties.putAll(props);
            config.externalFileLoaded = true;
            config.loadedSource = "MANUAL_TEST_PROPERTIES";
        }
        return config;
    }

    /**
     * Dış dosya sistemini tarayarak yapılandırma dosyasını arar ve yükler.
     * Derleme gerektirmemesi için classpath içi gömülü dosyalara bakmaz.
     */
    public synchronized void reload() {
        properties.clear();
        externalFileLoaded = false;
        searchedLocations.clear();
        validationErrors.clear();

        List<Path> candidatePaths = new ArrayList<>();

        // 1. Sistem Özelliği ile belirtilen özel dosya yolu (-Dconfig.file=...)
        String customFile = System.getProperty("config.file");
        if (customFile != null && !customFile.isBlank()) {
            candidatePaths.add(Paths.get(customFile));
        }

        // 2. Çalışma dizini ve olası konumlar
        candidatePaths.add(Paths.get("application.properties"));
        candidatePaths.add(Paths.get(nodeId + ".properties"));
        candidatePaths.add(Paths.get(nodeId, "application.properties"));
        candidatePaths.add(Paths.get(nodeId, nodeId + ".properties"));
        candidatePaths.add(Paths.get("..", nodeId, "application.properties"));
        candidatePaths.add(Paths.get(System.getProperty("user.dir"), "application.properties"));
        candidatePaths.add(Paths.get(System.getProperty("user.dir"), nodeId, "application.properties"));

        for (Path path : candidatePaths) {
            Path absPath = path.toAbsolutePath().normalize();
            String pathStr = absPath.toString();
            if (!searchedLocations.contains(pathStr)) {
                searchedLocations.add(pathStr);
            }

            if (Files.exists(absPath) && Files.isRegularFile(absPath)) {
                try (InputStream is = new FileInputStream(absPath.toFile())) {
                    properties.load(is);
                    externalFileLoaded = true;
                    loadedSource = absPath.toString();
                    break; // İlk bulunan dış dosyayı yükle
                } catch (Exception e) {
                    validationErrors.add(String.format("Yapılandırma dosyası okunamadı (%s): %s", absPath, e.getMessage()));
                }
            }
        }
    }

    // =========================================================================
    // KATI DOĞRULAMA (STRICT VALIDATION) VE HATA YÖNETİMİ
    // =========================================================================

    /**
     * Dış yapılandırma dosyasının varlığını zorunlu kılar.
     * Dosya bulunamazsa modal uyarı penceresi gösterir ve onaylanınca uygulamayı kapatır.
     */
    public void requireExternalFile(String nodeTitle) {
        if (!externalFileLoaded) {
            StringBuilder sb = new StringBuilder();
            sb.append("Uygulama başlatılamadı çünkü zorunlu dış yapılandırma dosyası (application.properties) bulunamadı!\n\n");
            sb.append("Aranan Dosya Konumları:\n");
            for (String loc : searchedLocations) {
                sb.append(" • ").append(loc).append("\n");
            }
            sb.append("\nLütfen çalışma dizininde geçerli bir 'application.properties' dosyası bulunduğundan emin olunuz.");

            ConfigErrorHandler.showFatalErrorAndExit(nodeTitle + " - Yapılandırma Dosyası Bulunamadı", sb.toString());
        }
    }

    /**
     * Zorunlu metin parametresini doğrular.
     */
    public String getRequiredString(String key) {
        String val = getString(key, null);
        if (val == null || val.isBlank()) {
            validationErrors.add(String.format("Zorunlu parametre eksik veya boş: '%s'", key));
            return "";
        }
        return val;
    }

    /**
     * Zorunlu Topic adını ve mimari önek kuralını doğrular ('core.' veya 'stream.').
     */
    public String getRequiredTopic(String key, String expectedPrefix) {
        String val = getRequiredString(key);
        if (!val.isBlank() && expectedPrefix != null && !val.startsWith(expectedPrefix)) {
            validationErrors.add(String.format("Geçersiz topic adı '%s' = '%s'. Mimari kural gereği konu '%s' öneki ile başlamalıdır!",
                    key, val, expectedPrefix));
        }
        return val;
    }

    /**
     * Zorunlu tamsayı parametresini aralık kontrolüyle doğrular.
     */
    public int getRequiredInt(String key, int min, int max) {
        String val = getString(key, null);
        if (val == null || val.isBlank()) {
            validationErrors.add(String.format("Zorunlu tamsayı parametresi eksik: '%s'", key));
            return min;
        }
        try {
            int num = Integer.parseInt(val);
            if (num < min || num > max) {
                validationErrors.add(String.format("'%s' = %d değeri sınırların dışında! (Beklenen: %d - %d arası)", key, num, min, max));
            }
            return num;
        } catch (NumberFormatException e) {
            validationErrors.add(String.format("'%s' için geçerli bir tamsayı girilmelidir. Okunan değer: '%s'", key, val));
            return min;
        }
    }

    /**
     * Zorunlu ondalıklı sayı parametresini aralık kontrolüyle doğrular.
     */
    public double getRequiredDouble(String key, double min, double max) {
        String val = getString(key, null);
        if (val == null || val.isBlank()) {
            validationErrors.add(String.format("Zorunlu ondalıklı sayı parametresi eksik: '%s'", key));
            return min;
        }
        try {
            double num = Double.parseDouble(val);
            if (num < min || num > max) {
                validationErrors.add(String.format("'%s' = %.2f değeri sınırların dışında! (Beklenen: %.2f - %.2f arası)", key, num, min, max));
            }
            return num;
        } catch (NumberFormatException e) {
            validationErrors.add(String.format("'%s' için geçerli bir ondalıklı sayı girilmelidir. Okunan değer: '%s'", key, val));
            return min;
        }
    }

    /**
     * Zorunlu boolean parametresini doğrular.
     */
    public boolean getRequiredBoolean(String key) {
        String val = getString(key, null);
        if (val == null || val.isBlank()) {
            validationErrors.add(String.format("Zorunlu boolean (true/false) parametresi eksik: '%s'", key));
            return false;
        }
        if ("true".equalsIgnoreCase(val) || "1".equals(val) || "yes".equalsIgnoreCase(val)) return true;
        if ("false".equalsIgnoreCase(val) || "0".equals(val) || "no".equalsIgnoreCase(val)) return false;

        validationErrors.add(String.format("'%s' için geçerli bir boolean (true/false) girilmelidir. Okunan değer: '%s'", key, val));
        return false;
    }

    /**
     * Toplanan tüm yapılandırma hatalarını kontrol eder.
     * Hata varsa modal hata penceresi açar ve onaylanınca uygulamayı kapatır.
     */
    public void validateAndEnforce(String nodeTitle) {
        if (!validationErrors.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Yapılandırma dosyasında (application.properties) hatalar tespit edildi:\n\n");
            for (String err : validationErrors) {
                sb.append(" ❌ ").append(err).append("\n");
            }
            sb.append("\nKaynak Dosya: ").append(loadedSource).append("\n");
            sb.append("Lütfen hataları düzelterek uygulamayı tekrar başlatınız.");

            ConfigErrorHandler.showFatalErrorAndExit(nodeTitle + " - Yapılandırma Hatası", sb.toString());
        }
    }

    // =========================================================================
    // STANDART OKUMA METOTLARI
    // =========================================================================

    public String getString(String key, String defaultValue) {
        String val = System.getProperty(key);
        if (val != null && !val.isBlank()) return val.trim();

        String envKey = key.replace('.', '_').toUpperCase();
        val = System.getenv(envKey);
        if (val != null && !val.isBlank()) return val.trim();

        val = properties.getProperty(key);
        if (val != null && !val.isBlank()) return val.trim();

        return defaultValue;
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public int getInt(String key, int defaultValue) {
        String val = getString(key, null);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String val = getString(key, null);
        if (val == null) return defaultValue;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public double getDouble(String key, double defaultValue) {
        String val = getString(key, null);
        if (val == null) return defaultValue;
        try {
            return Double.parseDouble(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = getString(key, null);
        if (val == null) return defaultValue;
        return Boolean.parseBoolean(val) || "1".equals(val) || "yes".equalsIgnoreCase(val);
    }

    public List<String> getStringList(String key, List<String> defaultList) {
        String val = getString(key, null);
        if (val == null || val.isBlank()) return defaultList;

        String[] parts = val.split(",");
        List<String> list = new ArrayList<>();
        for (String p : parts) {
            if (!p.isBlank()) {
                list.add(p.trim());
            }
        }
        return list.isEmpty() ? defaultList : list;
    }

    public boolean hasKey(String key) {
        return getString(key, null) != null;
    }

    public boolean isExternalFileLoaded() {
        return externalFileLoaded;
    }

    public String getLoadedSource() {
        return loadedSource;
    }

    public List<String> getValidationErrors() {
        return Collections.unmodifiableList(validationErrors);
    }

    public Properties getAllProperties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    @Override
    public String toString() {
        return String.format("NodeConfig[nodeId=%s, source=%s, externalLoaded=%b, keys=%d]",
                nodeId, loadedSource, externalFileLoaded, properties.size());
    }
}
