package com.stream.base.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class NodeConfigTest {

    @Test
    public void testDefaultAndManualProperties() {
        Properties props = new Properties();
        props.setProperty("nats.url", "nats://remote-server:4222");
        props.setProperty("publish.interval.ms", "2500");
        props.setProperty("feature.enabled", "true");
        props.setProperty("speed.threshold", "120.5");
        props.setProperty("nats.stream.subjects", "stream.a,stream.b,stream.c");

        NodeConfig config = NodeConfig.fromProperties("test-node", props);

        assertEquals("nats://remote-server:4222", config.getString("nats.url", "default"));
        assertEquals(2500, config.getInt("publish.interval.ms", 1000));
        assertTrue(config.getBoolean("feature.enabled", false));
        assertEquals(120.5, config.getDouble("speed.threshold", 0.0));
        assertEquals(3, config.getStringList("nats.stream.subjects", null).size());

        // Olmayan anahtar varsayılan değeri dönmeli
        assertEquals("default-stream", config.getString("non.existent.key", "default-stream"));
        assertEquals(999, config.getInt("non.existent.int", 999));
    }

    @Test
    public void testSystemPropertyOverride() {
        Properties props = new Properties();
        props.setProperty("nats.url", "nats://initial-server:4222");

        NodeConfig config = NodeConfig.fromProperties("test-node", props);

        // System property set edilirse dosyayı ezmeli
        System.setProperty("nats.url", "nats://system-override:4222");
        try {
            assertEquals("nats://system-override:4222", config.getString("nats.url", "default"));
        } finally {
            System.clearProperty("nats.url");
        }
    }

    @Test
    public void testRequireExternalFileFailsWhenNotLoaded() {
        ConfigErrorHandler.exitOnFatalError = false;
        try {
            // Var olmayan bir node için yükleme denemesi
            NodeConfig config = new NodeConfig("non-existent-node-12345");
            assertThrows(ConfigValidationException.class, () -> config.requireExternalFile("Test Node"));
        } finally {
            ConfigErrorHandler.exitOnFatalError = true;
        }
    }

    @Test
    public void testStrictValidationValidParameters() {
        Properties props = new Properties();
        props.setProperty("test.topic.core", "core.sample.event");
        props.setProperty("test.topic.stream", "stream.sample.event");
        props.setProperty("test.interval", "500");
        props.setProperty("test.ratio", "75.5");
        props.setProperty("test.flag", "true");

        NodeConfig config = NodeConfig.fromProperties("test-node", props);

        assertEquals("core.sample.event", config.getRequiredTopic("test.topic.core", "core."));
        assertEquals("stream.sample.event", config.getRequiredTopic("test.topic.stream", "stream."));
        assertEquals(500, config.getRequiredInt("test.interval", 100, 1000));
        assertEquals(75.5, config.getRequiredDouble("test.ratio", 0.0, 100.0));
        assertTrue(config.getRequiredBoolean("test.flag"));

        assertTrue(config.getValidationErrors().isEmpty(), "Geçerli parametrelerde hata listesi boş olmalıdır.");
        assertDoesNotThrow(() -> config.validateAndEnforce("Test Node"));
    }

    @Test
    public void testStrictValidationCatchesInvalidValues() {
        ConfigErrorHandler.exitOnFatalError = false;
        try {
            Properties props = new Properties();
            props.setProperty("test.topic.core", "wrong.sample.event"); // 'core.' ile başlamıyor
            props.setProperty("test.interval", "50"); // 100-1000 aralığının altında
            props.setProperty("test.ratio", "abc"); // Geçersiz double
            props.setProperty("test.flag", "maybe"); // Geçersiz boolean

            NodeConfig config = NodeConfig.fromProperties("test-node", props);

            config.getRequiredTopic("test.topic.core", "core.");
            config.getRequiredInt("test.interval", 100, 1000);
            config.getRequiredDouble("test.ratio", 0.0, 100.0);
            config.getRequiredBoolean("test.flag");
            config.getRequiredString("missing.key");

            assertEquals(5, config.getValidationErrors().size());
            assertThrows(ConfigValidationException.class, () -> config.validateAndEnforce("Test Node"));
        } finally {
            ConfigErrorHandler.exitOnFatalError = true;
        }
    }
}

