package com.stream.base;

import org.junit.jupiter.api.Test;
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
