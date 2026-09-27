package com.stream.node1;

import com.stream.base.BaseNode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AppTest {

    @Test
    public void testAppExtendsBaseNode() {
        App app = new App();
        
        // BaseNode sınıfını extend ettiğini doğrula
        assertTrue(app instanceof BaseNode, "App sınıfı BaseNode sınıfını extend etmelidir.");
        assertEquals("node-1", app.getNodeId(), "Node ID 'node-1' olmalıdır.");
    }

    @Test
    public void testDeviceStateSerialization() {
        App app = new App();
        App.DeviceState state = new App.DeviceState("drone-01", "READY", 98.2);
        
        String json = app.toJson(state);
        assertNotNull(json);
        assertTrue(json.contains("drone-01"));
        assertTrue(json.contains("READY"));

        App.DeviceState parsed = app.fromJson(json, App.DeviceState.class);
        assertEquals("drone-01", parsed.deviceId);
        assertEquals("READY", parsed.status);
        assertEquals(98.2, parsed.batteryLevel);
    }

    @Test
    public void testCoreCommandSerialization() {
        App app = new App();
        App.CoreCommand cmd = new App.CoreCommand("REBOOT", "control-center");
        
        String json = app.toJson(cmd);
        assertNotNull(json);
        assertTrue(json.contains("REBOOT"));

        App.CoreCommand parsed = app.fromJson(json, App.CoreCommand.class);
        assertEquals("REBOOT", parsed.action);
        assertEquals("control-center", parsed.sender);
    }
}

