package com.stream.node1;

import com.stream.base.BaseNode;
import com.stream.base.model.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AppTest {

    @Test
    public void testAppExtendsBaseNode() {
        App app = new App();
        assertTrue(app instanceof BaseNode, "App sınıfı BaseNode sınıfını extend etmelidir.");
        assertEquals("node-1", app.getNodeId(), "Node ID 'node-1' olmalıdır.");
    }

    @Test
    public void testModelSerialization() {
        App app = new App();

        // 1. TelemetryData
        TelemetryData telemetry = new TelemetryData(90.0, 2800, 45.0, 5);
        String jsonTelemetry = app.toJson(telemetry);
        assertTrue(jsonTelemetry.contains("90.0"));
        TelemetryData parsedTel = app.fromJson(jsonTelemetry, TelemetryData.class);
        assertEquals(90.0, parsedTel.speedKmh);

        // 2. VehicleStatus
        VehicleStatus status = new VehicleStatus(75.0, 90.0, 25000.0, 89.5);
        String jsonStatus = app.toJson(status);
        assertTrue(jsonStatus.contains("25000.0"));

        // 3. AlertData
        AlertData alert = new AlertData("EMERGENCY_BRAKE", "CRITICAL", "Ön çarpışma önlendi");
        String jsonAlert = app.toJson(alert);
        assertTrue(jsonAlert.contains("CRITICAL"));

        // 4. MaintenanceRecord
        MaintenanceRecord maint = new MaintenanceRecord(20000.0, 85.0, 90.0, true, "Rutin bakım");
        String jsonMaint = app.toJson(maint);
        assertTrue(jsonMaint.contains("Rutin bakım"));
    }

    @Test
    public void testBusinessLogicDriverCommand() {
        App app = new App();
        double initialSpeed = app.getCurrentSpeedKmh();

        // Gaz verme komutu
        app.handleDriverCommand(new DriverCommand("ACCELERATE", 60.0, "human-pedal"));
        assertTrue(app.getCurrentSpeedKmh() > initialSpeed, "Hızlanma komutundan sonra hız artmalıdır.");

        // Fren komutu
        double fastSpeed = app.getCurrentSpeedKmh();
        app.handleDriverCommand(new DriverCommand("BRAKE", 50.0, "human-brake"));
        assertTrue(app.getCurrentSpeedKmh() < fastSpeed, "Fren komutundan sonra hız düşmelidir.");
    }

    @Test
    public void testBusinessLogicEmergencyBrakingOnRadarObstacle() {
        App app = new App();

        // Hızı 80 km/h seviyesine çıkar
        app.handleDriverCommand(new DriverCommand("ACCELERATE", 80.0, "pedal"));
        double preRadarSpeed = app.getCurrentSpeedKmh();

        // Ön radarda 10 metre mesafede kritik engel tespit edilsin
        RadarSensorData criticalRadar = new RadarSensorData(10.0, -40.0, true);
        app.handleRadarSensor(criticalRadar);

        // Otomatik acil fren devreye girmeli ve hız belirgin şekilde düşmeli
        assertTrue(app.getCurrentSpeedKmh() < preRadarSpeed, "Kritik radar engelinde hız acil frenle düşürülmelidir.");
    }

    @Test
    public void testBusinessLogicDriverProfile() {
        App app = new App();
        app.handleDriverProfile(new DriverProfile("SPORT", 220.0, true), null);
        assertEquals("SPORT", app.getActiveProfile().mode);
        assertEquals(220.0, app.getActiveProfile().speedLimitKmh);
    }
}
