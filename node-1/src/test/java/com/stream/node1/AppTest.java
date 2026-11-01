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

    @Test
    public void testConfigDrivenTopics() {
        App app = new App();
        // application.properties dosyasından yüklenen topicler doğrulanır
        assertEquals("core.car.telemetry", app.getTopicTelemetry());
        assertEquals("stream.car.status", app.getTopicStatus());
        assertEquals("core.car.alerts", app.getTopicAlerts());
        assertEquals("core.car.commands", app.getTopicCommands());
        assertEquals("core.car.sensors.radar", app.getTopicRadar());
        assertEquals("stream.weather.current", app.getTopicWeather());
        assertEquals("stream.driver.profile", app.getTopicProfile());
        assertEquals("stream.car.maintenance", app.getTopicMaintenance());
        assertEquals(1000, app.getPublishIntervalMs());
    }

    private java.util.Properties createFullValidProperties() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("publish.telemetry.topic", "core.car.telemetry");
        props.setProperty("publish.status.topic", "stream.car.status");
        props.setProperty("publish.alerts.topic", "core.car.alerts");
        props.setProperty("publish.interval.ms", "1000");
        props.setProperty("subscribe.commands.topic", "core.car.commands");
        props.setProperty("subscribe.radar.topic", "core.car.sensors.radar");
        props.setProperty("subscribe.weather.topic", "stream.weather.current");
        props.setProperty("subscribe.profile.topic", "stream.driver.profile");
        props.setProperty("history.maintenance.topic", "stream.car.maintenance");
        props.setProperty("vehicle.radar.emergency.distance.meters", "20.0");
        props.setProperty("vehicle.radar.emergency.speed.kmh", "30.0");
        return props;
    }

    @Test
    public void testCustomConfigInjection() {
        java.util.Properties customProps = createFullValidProperties();
        customProps.setProperty("publish.telemetry.topic", "core.custom.telemetry");
        customProps.setProperty("publish.interval.ms", "500");

        com.stream.base.config.NodeConfig customConfig = com.stream.base.config.NodeConfig.fromProperties("node-1", customProps);
        App app = new App(null, customConfig);

        assertEquals("core.custom.telemetry", app.getTopicTelemetry());
        assertEquals(500, app.getPublishIntervalMs());
    }

    @Test
    public void testConfigMissingPropertyFailsValidation() {
        com.stream.base.config.ConfigErrorHandler.exitOnFatalError = false;
        try {
            java.util.Properties brokenProps = createFullValidProperties();
            brokenProps.remove("publish.telemetry.topic");

            com.stream.base.config.NodeConfig customConfig = com.stream.base.config.NodeConfig.fromProperties("node-1", brokenProps);
            assertThrows(com.stream.base.config.ConfigValidationException.class, () -> new App(null, customConfig));
        } finally {
            com.stream.base.config.ConfigErrorHandler.exitOnFatalError = true;
        }
    }

    @Test
    public void testConfigInvalidTopicPrefixFailsValidation() {
        com.stream.base.config.ConfigErrorHandler.exitOnFatalError = false;
        try {
            java.util.Properties brokenProps = createFullValidProperties();
            brokenProps.setProperty("publish.status.topic", "wrongprefix.car.status");

            com.stream.base.config.NodeConfig customConfig = com.stream.base.config.NodeConfig.fromProperties("node-1", brokenProps);
            assertThrows(com.stream.base.config.ConfigValidationException.class, () -> new App(null, customConfig));
        } finally {
            com.stream.base.config.ConfigErrorHandler.exitOnFatalError = true;
        }
    }

    @Test
    public void testConfigOutOfRangeNumberFailsValidation() {
        com.stream.base.config.ConfigErrorHandler.exitOnFatalError = false;
        try {
            java.util.Properties brokenProps = createFullValidProperties();
            brokenProps.setProperty("publish.interval.ms", "10"); // Min 50 olmalı

            com.stream.base.config.NodeConfig customConfig = com.stream.base.config.NodeConfig.fromProperties("node-1", brokenProps);
            assertThrows(com.stream.base.config.ConfigValidationException.class, () -> new App(null, customConfig));
        } finally {
            com.stream.base.config.ConfigErrorHandler.exitOnFatalError = true;
        }
    }

    @Test
    public void testFullSetBurstDemoEmitsSingleAggregatedMessage() {
        App app = new App();
        try {
            app.simulateFullSetBurstDemo();
            assertEquals(1L, app.getDemoAggregator().getFlushCount(),
                    "4 farklı topic arka arkaya geldiğinde sadece 1 kez birleşik yayın çıkmalıdır.");
        } finally {
            app.stop();
        }
    }

    @Test
    public void testCollisionBurstDemoFlushesPreviousValuesImmediately() {
        App app = new App();
        try {
            // T1 (topicCommands) üst üste 3 kez gönderilir -> İlk 2'si çakışma ile anında çıkmalı, 3.'sü pencerede beklemeli
            app.simulateCollisionBurstDemo();
            assertEquals(2L, app.getDemoAggregator().getFlushCount(),
                    "Üst üste 3 T1 geldiğinde önceki 2 T1 ezilmeden anında yayınlanmalıdır.");
        } finally {
            app.stop();
        }
    }
}
