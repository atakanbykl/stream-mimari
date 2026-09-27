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
    public void testSensorDataSerialization() {
        App app = new App();
        App.SensorData sensorData = new App.SensorData("temp-sensor-1", 25.4);
        
        String json = app.toJson(sensorData);
        assertNotNull(json);
        assertTrue(json.contains("temp-sensor-1"));

        App.SensorData parsed = app.fromJson(json, App.SensorData.class);
        assertEquals("temp-sensor-1", parsed.sensorId);
        assertEquals(25.4, parsed.temperature);
    }
}
