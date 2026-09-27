package com.stream.node1;

import com.stream.base.BaseNode;
import com.stream.base.StreamRecord;
import io.nats.client.Message;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * node-1 Servisi.
 * 
 * Bu sınıf çift katmanlı mimariyi uygular:
 * 1. NATS Core Katmanı:
 *    - Anlık, minimum gecikmeli komut ve telemetri alışverişi (in-memory).
 *    - Örnek: 'core.commands' konusu.
 * 
 * 2. JetStream Geçmiş Hafızası (Temporal Memory / State Store) Katmanı:
 *    - Olayların ve durumların kalıcı olarak saklanması.
 *    - Anlık canlı dinleme (yalnızca yeni mesajları alarak eski geçmişle boğulmama): subscribeStreamLive.
 *    - Geçmişten son mesajı anında çekme (Direct Get): getLastMessageJson / getLastMessageRecord.
 */
public class App extends BaseNode {

    // Örnek Durum Modeli (Geçmiş hafızasında saklanacak nesne)
    public static class DeviceState {
        public String deviceId;
        public String status;
        public double batteryLevel;
        public long timestamp;

        public DeviceState() {}

        public DeviceState(String deviceId, String status, double batteryLevel) {
            this.deviceId = deviceId;
            this.status = status;
            this.batteryLevel = batteryLevel;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return String.format("DeviceState[id=%s, status=%s, battery=%.1f%%]", deviceId, status, batteryLevel);
        }
    }

    // Örnek Anlık Komut Modeli (NATS Core üzerinden ultra hızlı iletilecek nesne)
    public static class CoreCommand {
        public String action;
        public String sender;

        public CoreCommand() {}

        public CoreCommand(String action, String sender) {
            this.action = action;
            this.sender = sender;
        }

        @Override
        public String toString() {
            return String.format("CoreCommand[action=%s, sender=%s]", action, sender);
        }
    }

    public App() {
        super("node-1");
    }

    public App(String natsUrl) {
        super("node-1", natsUrl);
    }

    @Override
    protected void onStart() throws Exception {
        log("===================================================================");
        log("Node-1 Başlatıldı. Çift katmanlı mesajlaşma altyapısı kuruluyor...");
        log("===================================================================");

        // =====================================================================
        // ADIM 1: JetStream Hafızasını Hazırlama (allowDirect=true ile)
        // =====================================================================
        // Tüm 'stream.>' konularını kapsayan merkezi mimari stream'ini garantiye al
        ensureDefaultStream();

        // =====================================================================
        // ADIM 2: NATS Core Katmanı (Minimum Gecikmeli Canlı Dinleme)
        // =====================================================================
        subscribeCoreJson("core.commands", CoreCommand.class, this::handleCoreCommand);

        // =====================================================================
        // ADIM 3: JetStream Canlı Dinleme (Yalnızca YENİ gelen olaylar)
        // =====================================================================
        // Geçmişteki 1000'lerce mesajı baştan okumaz, sadece bu andan itibaren gelenleri yakalar
        subscribeStreamLiveJson("stream.device.state", DeviceState.class, this::handleLiveStreamDeviceState, true);

        // =====================================================================
        // ADIM 4: Örnek Mesaj Yayınlama & Geçmişten Son Mesajı Sorgulama
        // =====================================================================
        simulateOperations();
    }

    /**
     * NATS Core üzerinden gelen ultra hızlı anlık komutları işler.
     */
    private void handleCoreCommand(CoreCommand cmd) {
        log("[CORE-ANLIK-GECİKMESİZ] Hızlı komut alındı: " + cmd);
    }

    /**
     * JetStream üzerinden gelen canlı durum güncellemelerini işler.
     */
    private void handleLiveStreamDeviceState(DeviceState state, Message msg) {
        log("[STREAM-CANLI-DİNLEME] Yeni cihaz durumu yakalandı: " + state);
    }

    /**
     * Örnek akış simülasyonu.
     */
    public void simulateOperations() {
        try {
            // 1. NATS Core üzerinden ultra hızlı bir komut yayınla
            publishCoreJson("core.commands", new CoreCommand("CALIBRATE_SENSORS", getNodeId()));
            log("Core NATS üzerinden anlık komut yayınlandı.");

            // 2. JetStream geçmiş hafızasına bir cihaz durumu kaydet
            DeviceState newState = new DeviceState("drone-alpha", "ARMED", 94.5);
            publishStreamJson("stream.device.state", newState);
            log("JetStream geçmiş hafızasına cihaz durumu kaydedildi: " + newState);

            // 3. KRİTİK İŞLEV: Herhangi bir zamanda geçmişten son mesajı çekme (Direct Get)
            fetchHistoricalLatestState("stream.device.state");

        } catch (Exception e) {
            log("İşlem simülasyonu sırasında hata: " + e.getMessage());
        }
    }

    /**
     * JetStream geçmiş hafızasından bir topic'in EN SON kaydedilen durumunu sorgular.
     */
    public void fetchHistoricalLatestState(String subject) {
        log(String.format("'%s' konusu için geçmiş hafızasındaki son durum sorgulanıyor...", subject));

        // Metadatalı kayıt olarak son mesajı çek
        Optional<StreamRecord<DeviceState>> recordOpt = getLastMessageRecord(subject, DeviceState.class);

        if (recordOpt.isPresent()) {
            StreamRecord<DeviceState> record = recordOpt.get();
            log(String.format("[GEÇMİŞTEN-SON-MESAJ] Bulundu! Sıra No: %d, Zaman: %s, Veri: %s",
                    record.getSequence(), record.getTimestamp(), record.getPayload()));
        } else {
            log(String.format("[GEÇMİŞTEN-SON-MESAJ] '%s' konusunda henüz kayıtlı bir geçmiş bulunamadı (Cold Start).", subject));
        }
    }

    @Override
    protected void onStop() throws Exception {
        log("Node-1 servis kaynakları temizlendi.");
    }

    public static void main(String[] args) {
        App node = new App();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        node.start();
    }
}

