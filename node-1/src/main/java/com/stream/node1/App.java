package com.stream.node1;

import com.stream.base.BaseNode;
import com.stream.base.StreamRecord;
import com.stream.base.model.*;
import com.stream.node1.gui.Node1Dashboard;
import com.stream.node1.gui.NodeActivityListener;
import io.nats.client.Message;

import javax.swing.SwingUtilities;
import java.awt.GraphicsEnvironment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * node-1 Servisi: Otomobil Araç Kontrol ve Telematik Ünitesi (ECU Gateway)
 * 
 * Bu servis çift katmanlı mimaride şu rolleri üstlenir:
 * 
 * 1. YAYINLAMA (PUBLISH - Belli aralıklarla 3 farklı topic):
 *    - P1: 'core.car.telemetry' (NATS Core) -> Hız, devir, vites gibi ultra hızlı canlı veriler.
 *    - P2: 'stream.car.status' (JetStream) -> Yakıt, batarya, kilometre, sıcaklık gibi kalıcı durum hafızası.
 *    - P3: 'core.car.alerts' (NATS Core) -> Acil fren, çarpışma riski, kaygan yol gibi anlık ikazlar.
 * 
 * 2. DİNLEME (SUBSCRIBE - 4 farklı topic ve her birine özel handler):
 *    - S1: 'core.car.commands' (NATS Core) -> Şoförden/otonom sistemden gelen anlık sürüş komutları.
 *    - S2: 'core.car.sensors.radar' (NATS Core) -> Ön radar sensöründen gelen anlık engel mesafesi.
 *    - S3: 'stream.weather.current' (JetStream Canlı) -> Dış ortam hava durumu ve buzlanma riski.
 *    - S4: 'stream.driver.profile' (JetStream Canlı) -> Sürüş modu profili (ECO, COMFORT, SPORT).
 * 
 * 3. GEÇMİŞTEN ÇEKME (DIRECT GET - İhtiyaç anında 1 topic):
 *    - 'stream.car.maintenance' -> JetStream geçmiş hafızasından aracın son bakım ve balata sağlığı durumunu sorgulama.
 * 
 * 4. İŞ MANTIĞI (BUSINESS LOGIC):
 *    - Radar mesafesi < 20m ve hız > 30 km/h ise acil fren (Emergency Brake) tetikler ve ikaz yayınlar.
 *    - Kaygan yol tespit edilirse çekiş kontrolünü (ESP) devreye alır.
 *    - Sürüş moduna göre gaz tepkisini ve tork sınırını dinamik ayarlar.
 *    - Geçmişten çekilen bakım kaydında balata ömrü kritikse uyarı verir ve maksimum hızı sınırlar.
 */
public class App extends BaseNode {

    // Otomobil Anlık Dinamik Durumu
    private double currentSpeedKmh = 50.0;
    private int currentRpm = 2000;
    private double currentThrottle = 25.0;
    private int currentGear = 3;
    private double fuelPercent = 82.5;
    private double batteryPercent = 95.0;
    private double odometerKm = 24150.0;
    private double engineTempCelsius = 88.0;

    // Harici Dinlenen Modeller
    private DriverProfile activeProfile = new DriverProfile("COMFORT", 130.0, false);
    private WeatherCondition currentWeather = new WeatherCondition("CLEAR", 22.0, false);
    private boolean maintenanceWarningActive = false;

    // GUI ve Dış Gözlem Dinleyicileri
    private final List<NodeActivityListener> activityListeners = new CopyOnWriteArrayList<>();

    // Konfigürasyondan dinamik okunan topic ve ayar alanları
    private final String topicTelemetry;
    private final String topicStatus;
    private final String topicAlerts;
    private final int publishIntervalMs;

    private final String topicCommands;
    private final String topicRadar;
    private final String topicWeather;
    private final String topicProfile;

    private final String topicMaintenance;

    private final double radarEmergencyDistance;
    private final double radarEmergencySpeed;

    // Periyodik yayınlayıcı zamanlayıcı
    private ScheduledExecutorService scheduler;

    public App() {
        this(null, null);
    }

    public App(String natsUrl) {
        this(natsUrl, null);
    }

    public App(String natsUrl, com.stream.base.config.NodeConfig customConfig) {
        super("node-1", (customConfig != null) ? customConfig : com.stream.base.config.NodeConfig.load("node-1"));

        // 1. ZORUNLU DIŞ DOSYA KONTROLÜ (Dosya yoksa kullanıcıya modal popup açıp kapatır)
        config.requireExternalFile("node-1 (Otomobil ECU)");

        // 2. TÜM ZORUNLU PARAMETRELERİN KATI DOĞRULAMASI
        this.topicTelemetry = config.getRequiredTopic("publish.telemetry.topic", "core.");
        this.topicStatus = config.getRequiredTopic("publish.status.topic", "stream.");
        this.topicAlerts = config.getRequiredTopic("publish.alerts.topic", "core.");
        this.publishIntervalMs = config.getRequiredInt("publish.interval.ms", 50, 60000);

        this.topicCommands = config.getRequiredTopic("subscribe.commands.topic", "core.");
        this.topicRadar = config.getRequiredTopic("subscribe.radar.topic", "core.");
        this.topicWeather = config.getRequiredTopic("subscribe.weather.topic", "stream.");
        this.topicProfile = config.getRequiredTopic("subscribe.profile.topic", "stream.");

        this.topicMaintenance = config.getRequiredTopic("history.maintenance.topic", "stream.");

        this.radarEmergencyDistance = config.getRequiredDouble("vehicle.radar.emergency.distance.meters", 1.0, 200.0);
        this.radarEmergencySpeed = config.getRequiredDouble("vehicle.radar.emergency.speed.kmh", 0.0, 300.0);

        // 3. EKSİK VEYA HATALI PARAMETRELERİ ENFORCE ET (Hata varsa kullanıcıya modal popup açıp kapatır)
        config.validateAndEnforce("node-1 (Otomobil ECU)");
    }

    @Override
    protected void onStart() throws Exception {
        log("===================================================================");
        log("OTOMOBİL ARAÇ KONTROL MERKEZİ (node-1) BAŞLATILDI");
        log("===================================================================");

        // 1. JetStream merkezi stream'ini garantiye al (allowDirect=true ile)
        ensureDefaultStream();

        // 2. DÖRT FARKLI TOPIC DİNLEME (Her birine özel atanmış handler)
        setupSubscribers();

        // 3. İHTİYAÇ DUYULDUĞUNDA GEÇMİŞTEN 1 TOPIC ÇEKME (Direct Get)
        // Araç marş aldığında geçmiş servis ve bakım hafızasını sorgula
        checkHistoricalMaintenance();

        // 4. BELLİ ARALIKLARLA 3 FARKLI TOPIC'E PERİYODİK YAYINLAMA
        startPeriodicTelemetryAndStatusPublisher();
    }

    // =========================================================================
    // GUI / DIŞ GÖZLEM DİNLEYİCİ YÖNETİMİ
    // =========================================================================

    public void addActivityListener(NodeActivityListener listener) {
        if (listener != null) activityListeners.add(listener);
    }

    public void removeActivityListener(NodeActivityListener listener) {
        activityListeners.remove(listener);
    }

    private void fireMessageReceived(String topic, String layer, Object payload) {
        String content = (payload instanceof String) ? (String) payload : toJson(payload);
        for (NodeActivityListener l : activityListeners) {
            try { l.onMessageReceived(topic, layer, content); } catch (Exception ignored) {}
        }
    }

    private void fireMessageSent(String topic, String layer, Object payload) {
        String content = (payload instanceof String) ? (String) payload : toJson(payload);
        for (NodeActivityListener l : activityListeners) {
            try { l.onMessageSent(topic, layer, content); } catch (Exception ignored) {}
        }
    }

    private void fireHistoricalRetrieved(String topic, long sequence, String timestamp, Object payload) {
        String content = (payload instanceof String) ? (String) payload : toJson(payload);
        for (NodeActivityListener l : activityListeners) {
            try { l.onHistoricalRetrieved(topic, sequence, timestamp, content); } catch (Exception ignored) {}
        }
    }

    private void fireStateChanged() {
        for (NodeActivityListener l : activityListeners) {
            try { l.onStateChanged(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // 1. DİNLENEN 4 FARKLI TOPIC VE HANDLER'LARI
    // =========================================================================

    private void setupSubscribers() throws Exception {
        // [Dinleyici 1 - Core]: Şoför komutları
        subscribeCoreJson(topicCommands, DriverCommand.class, this::handleDriverCommand);

        // [Dinleyici 2 - Core]: Ön radar sensörü
        subscribeCoreJson(topicRadar, RadarSensorData.class, this::handleRadarSensor);

        // [Dinleyici 3 - JetStream Canlı + Son Mesaj]: Dış hava durumu (geçmişteki son durumu alıp ardından canlı dinler)
        boolean weatherAutoAck = config.getBoolean("subscribe.weather.auto.ack", true);
        subscribeStreamLiveWithLastJson(topicWeather, WeatherCondition.class, this::handleWeatherUpdate, weatherAutoAck);

        // [Dinleyici 4 - JetStream Canlı + Son Mesaj]: Şoför sürüş modu profili (geçmişteki son profili alıp ardından canlı dinler)
        boolean profileAutoAck = config.getBoolean("subscribe.profile.auto.ack", true);
        subscribeStreamLiveWithLastJson(topicProfile, DriverProfile.class, this::handleDriverProfile, profileAutoAck);

        log(String.format("4 farklı topic konfigürasyondan başarıyla dinlemeye alındı: [%s, %s, %s, %s]",
                topicCommands, topicRadar, topicWeather, topicProfile));
    }

    /**
     * Handler 1 (Core): Şoförden gelen gaz/fren komutunu işler.
     */
    public void handleDriverCommand(DriverCommand cmd) {
        log(String.format("[İŞ MANTIĞI - KOMUT] Sürüş komutu alındı: %s (Kaynak: %s)", cmd.action, cmd.source));
        fireMessageReceived(topicCommands, "NATS Core", cmd);

        switch (cmd.action) {
            case "ACCELERATE" -> {
                double boostMultiplier = "SPORT".equals(activeProfile.mode) ? 1.5 : 1.0;
                if ("ECO".equals(activeProfile.mode)) boostMultiplier = 0.7;

                currentThrottle = Math.min(100.0, cmd.targetValue * boostMultiplier);
                currentSpeedKmh = Math.min(activeProfile.speedLimitKmh, currentSpeedKmh + (currentThrottle * 0.15));
                currentRpm = (int) (1000 + (currentSpeedKmh * 35));
                currentGear = Math.min(6, Math.max(1, (int) (currentSpeedKmh / 20) + 1));
                log(String.format("Hızlanıldı -> Hız: %.1f km/h, Devir: %d rpm, Vites: %d", currentSpeedKmh, currentRpm, currentGear));
            }
            case "BRAKE" -> {
                currentThrottle = 0.0;
                currentSpeedKmh = Math.max(0.0, currentSpeedKmh - (cmd.targetValue * 0.4));
                currentRpm = Math.max(800, (int) (1000 + (currentSpeedKmh * 25)));
                currentGear = Math.min(6, Math.max(1, (int) (currentSpeedKmh / 20) + 1));
                log(String.format("Fren yapıldı -> Hız: %.1f km/h, Devir: %d rpm", currentSpeedKmh, currentRpm));
            }
            default -> log("Bilinmeyen komut tipi: " + cmd.action);
        }
        fireStateChanged();
    }

    /**
     * Handler 2 (Core): Ön radar sensörü verisini işler.
     * İŞ MANTIĞI: Mesafe < radarEmergencyDistance ve hız > radarEmergencySpeed ise acil otomatik fren devreye girer!
     */
    public void handleRadarSensor(RadarSensorData radar) {
        fireMessageReceived(topicRadar, "NATS Core", radar);

        if (radar.obstacleDetected && radar.distanceMeters < radarEmergencyDistance && currentSpeedKmh > radarEmergencySpeed) {
            log(String.format("⚠️ [KRİTİK GÜVENLİK İKAZI] Ön tampon radarı engel tespit etti! Mesafe: %.1fm", radar.distanceMeters));

            // Acil durum frenlemesi (Emergency Braking)
            currentThrottle = 0.0;
            currentSpeedKmh = Math.max(10.0, currentSpeedKmh - 35.0);
            currentRpm = 1200;

            // P3 Konusu: Anlık Core İkaz yayınla
            AlertData alert = new AlertData("EMERGENCY_BRAKE", "CRITICAL",
                    String.format("Çarpışma önleyici acil fren devrede! Kalan mesafe: %.1f metre", radar.distanceMeters));
            publishCoreJson(topicAlerts, alert);
            fireMessageSent(topicAlerts, "NATS Core", alert);
        }
        fireStateChanged();
    }

    /**
     * Handler 3 (JetStream): Canlı hava durumunu işler.
     * İŞ MANTIĞI: Yol kaygan veya buzlu ise çekiş kontrolünü (ESP) ayarlar.
     */
    public void handleWeatherUpdate(WeatherCondition weather, Message msg) {
        this.currentWeather = weather;
        log(String.format("[İŞ MANTIĞI - HAVA DURUMU] Güncel yol koşulu: %s, Kayganlık: %b", weather.condition, weather.slipperyRoad));
        fireMessageReceived(topicWeather, "JetStream", weather);

        if (weather.slipperyRoad || "ICY".equals(weather.condition)) {
            log("Yol kaygan: ESP çekiş kontrolü maksimum güvenlik moduna geçirildi.");
            AlertData espAlert = new AlertData("ESP_ACTIVE", "MEDIUM", "Kaygan yol: Çekiş kontrolü (ESP) devrede.");
            publishCoreJson(topicAlerts, espAlert);
            fireMessageSent(topicAlerts, "NATS Core", espAlert);
        }
        fireStateChanged();
    }

    /**
     * Handler 4 (JetStream): Şoför profilini işler.
     */
    public void handleDriverProfile(DriverProfile profile, Message msg) {
        this.activeProfile = profile;
        log(String.format("[İŞ MANTIĞI - PROFİL] Sürüş modu değişti: %s, Maksimum Hız Sınırı: %.0f km/h",
                profile.mode, profile.speedLimitKmh));
        fireMessageReceived(topicProfile, "JetStream", profile);
        fireStateChanged();
    }

    // =========================================================================
    // 2. İHTİYAÇ ANINDA GEÇMİŞTEN 1 TOPIC ÇEKME (DIRECT GET)
    // =========================================================================

    /**
     * JetStream geçmiş hafızasından aracın son bakım durumunu (topicMaintenance) sorgular.
     */
    public void checkHistoricalMaintenance() {
        log("Geçmiş hafızasından son bakım ve servis kaydı sorgulanıyor (Topic: " + topicMaintenance + ")...");

        Optional<StreamRecord<MaintenanceRecord>> recordOpt =
                getLastMessageRecord(topicMaintenance, MaintenanceRecord.class);

        double minBrakePad = config.getDouble("vehicle.maintenance.min.brakepad.percent", 25.0);
        double minOilLife = config.getDouble("vehicle.maintenance.min.oillife.percent", 20.0);

        if (recordOpt.isPresent()) {
            StreamRecord<MaintenanceRecord> record = recordOpt.get();
            MaintenanceRecord maint = record.getPayload();
            log(String.format("Geçmiş bakım kaydı bulundu [Kayıt Zamanı: %s, Sıra: %d]: %s",
                    record.getTimestamp(), record.getSequence(), maint));

            fireHistoricalRetrieved(topicMaintenance, record.getSequence(), record.getTimestamp().toString(), maint);

            // İş Mantığı Değerlendirmesi
            if (maint.brakePadHealthPercent < minBrakePad || maint.oilLifePercent < minOilLife || !maint.inspectionPassed) {
                maintenanceWarningActive = true;
                log("⚠️ [DİKKAT] Geçmiş kayıtlara göre fren balatası veya motor yağı kritik eşikte!");
                AlertData alert = new AlertData("MAINTENANCE_REQUIRED", "MEDIUM",
                        String.format("Servis zamanı: Balata %% %.0f, Yağ %% %.0f", maint.brakePadHealthPercent, maint.oilLifePercent));
                publishCoreJson(topicAlerts, alert);
                fireMessageSent(topicAlerts, "NATS Core", alert);
            } else {
                log("Mekanik sağlık durumu iyi. Sürüşe hazır.");
            }
        } else {
            log("Geçmişte kayıtlı bakım bilgisi bulunamadı (Cold Start). Varsayılan fabrika kaydı oluşturuluyor...");
            fireHistoricalRetrieved(topicMaintenance, 0, "N/A", "Kayıt yok (Cold Start)");

            // İlk kez çalışan araç için geçmişe fabrika çıkış kaydı ekle
            MaintenanceRecord factoryDefault = new MaintenanceRecord(odometerKm, 98.0, 100.0, true, "Fabrika çıkışı ilk kontrol.");
            try {
                publishStreamJson(topicMaintenance, factoryDefault);
                fireMessageSent(topicMaintenance, "JetStream", factoryDefault);
                log("Fabrika bakım kaydı geçmiş hafızasına işlendi.");
            } catch (Exception e) {
                log("Bakım kaydı eklenirken hata: " + e.getMessage());
            }
        }
        fireStateChanged();
    }

    /**
     * JetStream kalıcı hafızasından bugüne kadar yayınlanmış tüm araç durumlarını
     * (topicStatus) Direct Get ile liste olarak çeker ve dinleyicilere (GUI vb.) aktarır.
     * BU SORGULAMA STREAM'DEKİ VERİLERİ ASLA DEĞİŞTİRMEZ VEYA SİLMEZ.
     */
    public void fetchAllHistoricalStatuses() {
        log("Geçmiş hafızasındaki tüm araç durum kayıtları taranıyor (Topic: " + topicStatus + ")...");
        List<StreamRecord<VehicleStatus>> records = getHistoricalRecords(topicStatus, VehicleStatus.class);
        log(String.format("Geçmiş hafızasından %d adet kalıcı araç durum kaydı başarıyla çekildi.", records.size()));
        for (StreamRecord<VehicleStatus> rec : records) {
            fireHistoricalRetrieved(topicStatus, rec.getSequence(), rec.getTimestamp().toString(), rec.getPayload());
        }
    }

    // =========================================================================
    // 3. BELLİ ARALIKLARLA 3 FARKLI TOPIC YAYINLAMA (PUBLISH LOOP)
    // =========================================================================

    private void startPeriodicTelemetryAndStatusPublisher() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "car-publisher-worker");
            t.setDaemon(true);
            return t;
        });

        // Konfigürasyondan okunan periyotla 3 konuyu yayınla
        scheduler.scheduleAtFixedRate(this::publishAllMetrics, publishIntervalMs, publishIntervalMs, TimeUnit.MILLISECONDS);
        log(String.format("Periyodik yayınlayıcı döngüsü başlatıldı (Aralık: %d ms, Kaynak: %s).",
                publishIntervalMs, config.getLoadedSource()));
    }

    /**
     * Belli aralıklarla tetiklenen ana yayın metodu:
     * 1. topicTelemetry (NATS Core)
     * 2. topicStatus (JetStream)
     * 3. topicAlerts (NATS Core)
     */
    public void publishAllMetrics() {
        try {
            // Küçük simülasyon hareketleri (kilometre ve yakıt tüketimi)
            if (currentSpeedKmh > 0) {
                odometerKm += (currentSpeedKmh / 3600.0);
                fuelPercent = Math.max(5.0, fuelPercent - 0.005);
            }

            // =================================================================
            // YAYIN 1 (NATS Core): Canlı Telemetri
            // =================================================================
            TelemetryData telemetry = new TelemetryData(currentSpeedKmh, currentRpm, currentThrottle, currentGear);
            publishCoreJson(topicTelemetry, telemetry);
            fireMessageSent(topicTelemetry, "NATS Core", telemetry);

            // =================================================================
            // YAYIN 2 (JetStream): Kalıcı Araç Durumu
            // =================================================================
            VehicleStatus status = new VehicleStatus(fuelPercent, batteryPercent, odometerKm, engineTempCelsius);
            publishStreamJson(topicStatus, status);
            fireMessageSent(topicStatus, "JetStream", status);

            // =================================================================
            // YAYIN 3 (NATS Core): Periyodik Güvenlik & İkaz Durumu
            // =================================================================
            if (maintenanceWarningActive) {
                AlertData alert = new AlertData("MAINTENANCE_DUE", "LOW", "Servis periyodu yaklaştı.");
                publishCoreJson(topicAlerts, alert);
                fireMessageSent(topicAlerts, "NATS Core", alert);
            } else if (fuelPercent < 15.0) {
                AlertData alert = new AlertData("LOW_FUEL", "MEDIUM", "Yakıt seviyesi düşük (%%" + String.format("%.1f", fuelPercent) + ")");
                publishCoreJson(topicAlerts, alert);
                fireMessageSent(topicAlerts, "NATS Core", alert);
            }

            log(String.format("[YAYIN TAMAMLANDI] %s | %s", telemetry, status));
            fireStateChanged();

        } catch (Exception e) {
            log("Periyodik yayın sırasında hata: " + e.getMessage());
        }
    }

    // =========================================================================
    // YAŞAM DÖNGÜSÜ KAPANIŞI
    // =========================================================================

    @Override
    protected void onStop() throws Exception {
        log("Otomobil Araç Kontrol Merkezi kapatılıyor, zamanlayıcılar durduruluyor...");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }
        log("Otomobil başarıyla park edildi ve kapatıldı.");
    }

    // Getter & Test yardımcıları
    public double getCurrentSpeedKmh() { return currentSpeedKmh; }
    public int getCurrentRpm() { return currentRpm; }
    public int getCurrentGear() { return currentGear; }
    public double getCurrentThrottle() { return currentThrottle; }
    public double getFuelPercent() { return fuelPercent; }
    public double getBatteryPercent() { return batteryPercent; }
    public double getOdometerKm() { return odometerKm; }
    public double getEngineTempCelsius() { return engineTempCelsius; }
    public DriverProfile getActiveProfile() { return activeProfile; }
    public WeatherCondition getCurrentWeather() { return currentWeather; }
    public boolean isMaintenanceWarningActive() { return maintenanceWarningActive; }

    // Konfigürasyondan okunan topic getter'ları
    public String getTopicTelemetry() { return topicTelemetry; }
    public String getTopicStatus() { return topicStatus; }
    public String getTopicAlerts() { return topicAlerts; }
    public String getTopicCommands() { return topicCommands; }
    public String getTopicRadar() { return topicRadar; }
    public String getTopicWeather() { return topicWeather; }
    public String getTopicProfile() { return topicProfile; }
    public String getTopicMaintenance() { return topicMaintenance; }
    public int getPublishIntervalMs() { return publishIntervalMs; }

    // =========================================================================
    // ANA ÇALIŞTIRICI METOT (MAIN ENTRY POINT)
    // =========================================================================

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println(">> node-1 (Connected Car ECU) Başlatılıyor... <<");
        System.out.println("=================================================");

        App carNode = new App();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n>> Kapatma sinyali alındı, node-1 durduruluyor... <<");
            carNode.stop();
        }));

        try {
            carNode.start();
            System.out.println(">> node-1 başarıyla ayağa kalktı ve dinlemede. <<");

            // Masaüstü ortamında GUI gösterge panelini aç
            if (!GraphicsEnvironment.isHeadless()) {
                SwingUtilities.invokeLater(() -> {
                    Node1Dashboard dashboard = new Node1Dashboard(carNode);
                    dashboard.setVisible(true);
                });
            }

            // Ana thread'in kapanmasını önleyip arka plan zamanlayıcı ve dinleyicilerini ayakta tut
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            carNode.stop();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println(">> node-1 başlatılamadı: " + e.getMessage());
            System.err.println(">> İpucu: NATS sunucusunun (nats-server) varsayılan 4222 portunda çalıştığından emin olun.");
        }
    }
}
