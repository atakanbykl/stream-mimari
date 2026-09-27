package com.stream.base;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BaseNodeTest {

    static class TestNode extends BaseNode {
        boolean started = false;
        boolean stopped = false;

        public TestNode() {
            super("test-node");
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
    public void testNodeLifecycle() {
        TestNode node = new TestNode();
        assertEquals("test-node", node.getNodeId());
        assertFalse(node.isRunning());

        node.start();
        assertTrue(node.isRunning());
        assertTrue(node.started);

        node.stop();
        assertFalse(node.isRunning());
        assertTrue(node.stopped);
    }
}
