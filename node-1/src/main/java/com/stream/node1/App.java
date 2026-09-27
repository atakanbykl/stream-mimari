package com.stream.node1;

import com.stream.base.BaseNode;
import com.stream.base.StreamRecord;
import com.stream.base.model.*;
import io.nats.client.Message;

import java.util.Optional;
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

    // Periyodik yayınlayıcı zamanlayıcı
    private ScheduledExecutorService scheduler;

    public App() {
        super("node-1");
    }

    public App(String natsUrl) {
        super("node-1", natsUrl);
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
    // 1. DİNLENEN 4 FARKLI TOPIC VE HANDLER'LARI
    // =========================================================================

    private void setupSubscribers() throws Exception {
        // [Dinleyici 1 - Core]: Şoför komutları
        subscribeCoreJson("core.car.commands", DriverCommand.class, this::handleDriverCommand);

        // [Dinleyici 2 - Core]: Ön radar sensörü
        subscribeCoreJson("core.car.sensors.radar", RadarSensorData.class, this::handleRadarSensor);

        // [Dinleyici 3 - JetStream Canlı]: Dış hava durumu (eski mesajları tekrar etmez)
        subscribeStreamLiveJson("stream.weather.current", WeatherCondition.class, this::handleWeatherUpdate, true);

        // [Dinleyici 4 - JetStream Canlı]: Şoför sürüş modu profili
        subscribeStreamLiveJson("stream.driver.profile", DriverProfile.class, this::handleDriverProfile, true);

        log("4 farklı topic başarıyla dinlemeye alındı.");
    }

    /**
     * Handler 1 (Core): Şoförden gelen gaz/fren komutunu işler.
     */
    public void handleDriverCommand(DriverCommand cmd) {
        log(String.format("[İŞ MANTIĞI - KOMUT] Sürüş komutu alındı: %s (Kaynak: %s)", cmd.action, cmd.source));

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
    }

    /**
     * Handler 2 (Core): Ön radar sensörü verisini işler.
     * İŞ MANTIĞI: Mesafe < 20m ve hız > 30 km/h ise acil otomatik fren devreye girer!
     */
    public void handleRadarSensor(RadarSensorData radar) {
        if (radar.obstacleDetected && radar.distanceMeters < 20.0 && currentSpeedKmh > 30.0) {
            log(String.format("⚠️ [KRİTİK GÜVENLİK İKAZI] Ön tampon radarı engel tespit etti! Mesafe: %.1fm", radar.distanceMeters));

            // Acil durum frenlemesi (Emergency Braking)
            currentThrottle = 0.0;
            currentSpeedKmh = Math.max(10.0, currentSpeedKmh - 35.0);
            currentRpm = 1200;

            // P3 Konusu: Anlık Core İkaz yayınla
            publishCoreJson("core.car.alerts", new AlertData("EMERGENCY_BRAKE", "CRITICAL",
                    String.format("Çarpışma önleyici acil fren devrede! Kalan mesafe: %.1f metre", radar.distanceMeters)));
        }
    }

    /**
     * Handler 3 (JetStream): Canlı hava durumunu işler.
     * İŞ MANTIĞI: Yol kaygan veya buzlu ise çekiş kontrolünü (ESP) ayarlar.
     */
    public void handleWeatherUpdate(WeatherCondition weather, Message msg) {
        this.currentWeather = weather;
        log(String.format("[İŞ MANTIĞI - HAVA DURUMU] Güncel yol koşulu: %s, Kayganlık: %b", weather.condition, weather.slipperyRoad));

        if (weather.slipperyRoad || "ICY".equals(weather.condition)) {
            log("Yol kaygan: ESP çekiş kontrolü maksimum güvenlik moduna geçirildi.");
            publishCoreJson("core.car.alerts", new AlertData("ESP_ACTIVE", "MEDIUM", "Kaygan yol: Çekiş kontrolü (ESP) devrede."));
        }
    }

    /**
     * Handler 4 (JetStream): Şoför profilini işler.
     */
    public void handleDriverProfile(DriverProfile profile, Message msg) {
        this.activeProfile = profile;
        log(String.format("[İŞ MANTIĞI - PROFİL] Sürüş modu değişti: %s, Maksimum Hız Sınırı: %.0f km/h",
                profile.mode, profile.speedLimitKmh));
    }

    // =========================================================================
    // 2. İHTİYAÇ ANINDA GEÇMİŞTEN 1 TOPIC ÇEKME (DIRECT GET)
    // =========================================================================

    /**
     * JetStream geçmiş hafızasından aracın son bakım durumunu ('stream.car.maintenance') sorgular.
     * İŞ MANTIĞI: Balata sağlığı < %25 veya yağ ömrü < %20 ise gösterge paneline bakım ikazı düşürür.
     */
    public void checkHistoricalMaintenance() {
        log("Geçmiş hafızasından son bakım ve servis kaydı sorgulanıyor (Topic: stream.car.maintenance)...");

        Optional<StreamRecord<MaintenanceRecord>> recordOpt =
                getLastMessageRecord("stream.car.maintenance", MaintenanceRecord.class);

        if (recordOpt.isPresent()) {
            StreamRecord<MaintenanceRecord> record = recordOpt.get();
            MaintenanceRecord maint = record.getPayload();
            log(String.format("Geçmiş bakım kaydı bulundu [Kayıt Zamanı: %s, Sıra: %d]: %s",
                    record.getTimestamp(), record.getSequence(), maint));

            // İş Mantığı Değerlendirmesi
            if (maint.brakePadHealthPercent < 25.0 || maint.oilLifePercent < 20.0 || !maint.inspectionPassed) {
                maintenanceWarningActive = true;
                log("⚠️ [DİKKAT] Geçmiş kayıtlara göre fren balatası veya motor yağı kritik eşikte!");
                publishCoreJson("core.car.alerts", new AlertData("MAINTENANCE_REQUIRED", "MEDIUM",
                        String.format("Servis zamanı: Balata %% %.0f, Yağ %% %.0f", maint.brakePadHealthPercent, maint.oilLifePercent)));
            } else {
                log("Mekanik sağlık durumu iyi. Sürüşe hazır.");
            }
        } else {
            log("Geçmişte kayıtlı bakım bilgisi bulunamadı (Cold Start). Varsayılan fabrika kaydı oluşturuluyor...");
            // İlk kez çalışan araç için geçmişe fabrika çıkış kaydı ekle
            MaintenanceRecord factoryDefault = new MaintenanceRecord(odometerKm, 98.0, 100.0, true, "Fabrika çıkışı ilk kontrol.");
            try {
                publishStreamJson("stream.car.maintenance", factoryDefault);
                log("Fabrika bakım kaydı geçmiş hafızasına işlendi.");
            } catch (Exception e) {
                log("Bakım kaydı eklenirken hata: " + e.getMessage());
            }
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

        // Her 1 saniyede bir 3 farklı konuyu yayınla
        scheduler.scheduleAtFixedRate(this::publishAllMetrics, 1, 1, TimeUnit.SECONDS);
        log("Periyodik yayınlayıcı döngüsü başlatıldı (Aralık: 1 saniye).");
    }

    /**
     * Belli aralıklarla tetiklenen ana yayın metodu:
     * 1. core.car.telemetry (NATS Core)
     * 2. stream.car.status (JetStream)
     * 3. core.car.alerts (NATS Core)
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
            publishCoreJson("core.car.telemetry", telemetry);

            // =================================================================
            // YAYIN 2 (JetStream): Kalıcı Araç Durumu
            // =================================================================
            VehicleStatus status = new VehicleStatus(fuelPercent, batteryPercent, odometerKm, engineTempCelsius);
            publishStreamJson("stream.car.status", status);

            // =================================================================
            // YAYIN 3 (NATS Core): Periyodik Güvenlik & İkaz Durumu
            // =================================================================
            if (maintenanceWarningActive) {
                publishCoreJson("core.car.alerts", new AlertData("MAINTENANCE_DUE", "LOW", "Servis periyodu yaklaştı."));
            } else if (fuelPercent < 15.0) {
                publishCoreJson("core.car.alerts", new AlertData("LOW_FUEL", "MEDIUM", "Yakıt seviyesi düşük (%%" + String.format("%.1f", fuelPercent) + ")"));
            }

            log(String.format("[YAYIN TAMAMLANDI] %s | %s", telemetry, status));

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
    public double getOdometerKm() { return odometerKm; }
    public DriverProfile getActiveProfile() { return activeProfile; }
    public boolean isMaintenanceWarningActive() { return maintenanceWarningActive; }

    public static void main(String[] args) {
        App carNode = new App();
        Runtime.getRuntime().addShutdownHook(new Thread(carNode::stop));
        carNode.start();
    }
}
