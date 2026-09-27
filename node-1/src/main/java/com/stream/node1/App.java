package com.stream.node1;

import com.stream.base.BaseNode;
import io.nats.client.Message;

import java.nio.charset.StandardCharsets;

/**
 * node-1 servisi. BaseNode sınıfını extend eder.
 * 
 * Bu sınıf:
 * - Hem NATS Core hem JetStream üzerinden birden fazla farklı topic'i dinler.
 * - Her topic'e atanmış özel handler metodları ile işlem yapar.
 * - Hem Core hem JetStream üzerinden birden fazla topic'e yayın yapar.
 */
public class App extends BaseNode {

    // Örnek veri modelleri
    public static class SensorData {
        public String sensorId;
        public double temperature;
        public long timestamp;

        public SensorData() {}
        public SensorData(String sensorId, double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public static class CommandData {
        public String targetNode;
        public String action;

        public CommandData() {}
        public CommandData(String targetNode, String action) {
            this.targetNode = targetNode;
            this.action = action;
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
        log("Servis başlatıldı. Topic abonelikleri tanımlanıyor...");

        // =====================================================================
        // 1. JETSTREAM STREAM HAZIRLIĞI
        // =====================================================================
        // JetStream topic'leri için stream tanımlanır (yoksa otomatik oluşturulur)
        ensureStream("STREAM_NODES", "stream.orders.*", "stream.alerts");

        // =====================================================================
        // 2. NATS CORE - ÇOKLU TOPIC DİNLEME (HER BİRİNE ÖZEL HANDLER)
        // =====================================================================
        
        // Topic A: "core.telemetry" için özel handler
        subscribeCore("core.telemetry", this::handleCoreTelemetry);

        // Topic B: "core.commands" için özel JSON handler
        subscribeCoreJson("core.commands", CommandData.class, this::handleCoreCommand);

        // =====================================================================
        // 3. JETSTREAM - ÇOKLU TOPIC DİNLEME (HER BİRİNE ÖZEL HANDLER)
        // =====================================================================

        // Stream Topic 1: "stream.orders.new" için özel handler (manuel ACK örneği)
        subscribeStream("stream.orders.new", this::handleStreamOrder, false);

        // Stream Topic 2: "stream.alerts" için özel JSON handler (otomatik ACK)
        subscribeStreamJson("stream.alerts", SensorData.class, this::handleStreamAlert, true);

        // =====================================================================
        // 4. ÇOKLU TOPIC'LERE YAYINLAMA (PUBLISH) ÖRNEKLERİ
        // =====================================================================
        sendSamplePublications();
    }

    // =========================================================================
    // HANDLER METOTLARI (Her topic için ayrı işlem yapan iş mantıkları)
    // =========================================================================

    private void handleCoreTelemetry(Message msg) {
        String data = new String(msg.getData(), StandardCharsets.UTF_8);
        log("[HANDLER-CORE-TELEMETRY] Ham telemetri verisi alındı: " + data);
    }

    private void handleCoreCommand(CommandData command) {
        log(String.format("[HANDLER-CORE-COMMAND] Komut alındı: target=%s, action=%s", 
                command.targetNode, command.action));
    }

    private void handleStreamOrder(Message msg) {
        String orderJson = new String(msg.getData(), StandardCharsets.UTF_8);
        log("[HANDLER-STREAM-ORDER] JetStream sipariş mesajı alındı: " + orderJson);
        // Manuel onaylama (ACK)
        msg.ack();
    }

    private void handleStreamAlert(SensorData alert, Message msg) {
        log(String.format("[HANDLER-STREAM-ALERT] JetStream sensör alarmı: %s -> %.2f °C", 
                alert.sensorId, alert.temperature));
    }

    // =========================================================================
    // FARKLI TOPIC'LERE YAYIN METOTLARI
    // =========================================================================

    public void sendSamplePublications() {
        try {
            // NATS Core: Farklı topic'lere yayın
            publishCore("core.telemetry", "Node-1 Core Telemetry verisi");
            publishCoreJson("core.commands", new CommandData("node-2", "START_SYNC"));

            // JetStream: Farklı topic'lere senkron / asenkron yayın
            publishStream("stream.orders.new", "{\"orderId\": \"ORD-1001\", \"item\": \"Laptop\"}");
            publishStreamJson("stream.alerts", new SensorData("sensor-node-1", 38.5));

            log("Örnek Core ve JetStream yayınları farklı topic'lere başarıyla gönderildi.");
        } catch (Exception e) {
            log("Yayın sırasında hata oluştu: " + e.getMessage());
        }
    }

    @Override
    protected void onStop() throws Exception {
        log("Node-1 özel durdurma işlemleri tamamlandı.");
    }

    public static void main(String[] args) {
        App node = new App();

        // Kapatma sinyali (Ctrl+C / SIGINT) yakalandığında temiz kapanış
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));

        node.start();
    }
}
