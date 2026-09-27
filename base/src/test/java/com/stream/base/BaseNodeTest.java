package com.stream.base;

import org.junit.jupiter.api.Test;
import java.time.ZonedDateTime;
import static org.junit.jupiter.api.Assertions.*;

public class BaseNodeTest {

    static class DummyNode extends BaseNode {
        boolean started = false;
        boolean stopped = false;

        public DummyNode() {
            super("test-dummy-node");
            // Birim test ortamında harici NATS sunucusuna bağlanmayı deneME
            setAutoConnect(false);
        }

        @Override
        protected void onStart() {
            started = true;
        }

        @Override
        protected void onStop() {
            stopped = true;
        }
    }

    @Test
    public void testLifecycle() {
        DummyNode node = new DummyNode();
        assertEquals("test-dummy-node", node.getNodeId());
        assertFalse(node.isRunning());

        node.start();
        assertTrue(node.isRunning());
        assertTrue(node.started);

        node.stop();
        assertFalse(node.isRunning());
        assertTrue(node.stopped);
    }

    @Test
    public void testJsonHelpers() {
        DummyNode node = new DummyNode();
        SamplePayload payload = new SamplePayload("sensor-1", 42.5);

        String json = node.toJson(payload);
        assertNotNull(json);
        assertTrue(json.contains("sensor-1"));

        SamplePayload deserialized = node.fromJson(json, SamplePayload.class);
        assertEquals("sensor-1", deserialized.name);
        assertEquals(42.5, deserialized.value);
    }

    @Test
    public void testStreamRecord() {
        SamplePayload payload = new SamplePayload("device-9", 100.0);
        ZonedDateTime now = ZonedDateTime.now();
        StreamRecord<SamplePayload> record = new StreamRecord<>(payload, 42L, now, "stream.device.state", null);

        assertEquals("stream.device.state", record.getSubject());
        assertEquals(42L, record.getSequence());
        assertEquals(now, record.getTimestamp());
        assertEquals("device-9", record.getPayload().name);
        assertNull(record.getHeaders());
    }

    @Test
    public void testResolveStreamNameDefault() {
        DummyNode node = new DummyNode();
        String stream = node.resolveStreamName("stream.orders.eur");
        assertEquals(BaseNode.DEFAULT_STREAM_NAME, stream);
    }

    @Test
    public void testTopicValidation() {
        DummyNode node = new DummyNode();

        // Geçerli konular
        assertDoesNotThrow(() -> node.validateCoreSubject("core.telemetry"));
        assertDoesNotThrow(() -> node.validateStreamSubject("stream.device.state"));

        // Katı doğrulama (strict validation) testi
        node.setStrictTopicValidation(true);

        // Core konusuna stream. öneki verilirse istisna fırlatmalı
        assertThrows(IllegalArgumentException.class, () -> node.validateCoreSubject("stream.wrong.core"));

        // Stream konusuna core. öneki verilirse istisna fırlatmalı
        assertThrows(IllegalArgumentException.class, () -> node.validateStreamSubject("core.wrong.stream"));

        // Boş konu istisna fırlatmalı
        assertThrows(IllegalArgumentException.class, () -> node.validateCoreSubject(""));
        assertThrows(IllegalArgumentException.class, () -> node.validateStreamSubject(null));
    }

    static class SamplePayload {
        String name;
        double value;

        public SamplePayload() {}
        public SamplePayload(String name, double value) {
            this.name = name;
            this.value = value;
        }
    }
}
