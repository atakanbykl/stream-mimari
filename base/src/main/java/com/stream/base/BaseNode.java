package com.stream.base;

import com.google.gson.Gson;
import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Tüm node-* servislerinin extend edeceği temel soyut sınıf.
 * 
 * Bu sınıf alt sınıflara şunları sağlar:
 * 1. NATS Core üzerinden birden fazla topic dinleme (her topic için özel handler).
 * 2. NATS Core üzerinden birden fazla farklı topic'e mesaj yayınlama (Publish).
 * 3. JetStream üzerinden birden fazla topic dinleme (her topic için özel handler).
 * 4. JetStream üzerinden birden fazla farklı topic'e mesaj yayınlama (Publish - sync / async).
 * 5. Stream yönetimi (ensureStream ile otomatik oluşturma).
 * 6. Gson tabanlı otomatik JSON serileştirme / deserileştirme.
 * 7. start() ve stop() ile thread-safe yaşam döngüsü ve kaynak temizliği.
 */
public abstract class BaseNode {

    private final String nodeId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String natsUrl = "nats://127.0.0.1:4222";
    private boolean autoConnect = true;

    // NATS ve JetStream nesneleri
    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jetStreamManagement;
    private Dispatcher coreDispatcher;
    private Dispatcher streamDispatcher;

    // Aktif abonelik kayıtları
    private final Map<String, Subscription> coreSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, JetStreamSubscription> streamSubscriptions = new ConcurrentHashMap<>();

    // JSON yardımcı sınıfı
    private final Gson gson = new Gson();

    public BaseNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public BaseNode(String nodeId, String natsUrl) {
        this.nodeId = nodeId;
        this.natsUrl = natsUrl;
    }

    // =========================================================================
    // YAŞAM DÖNGÜSÜ (LIFECYCLE) METOTLARI
    // =========================================================================

    /**
     * Düğümü başlatır. Bağlantıyı kurar ve ardından onStart() metodunu çağırır.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log("Başlatılıyor...");
            try {
                if (autoConnect) {
                    connect();
                }
                onStart();
                log("Başarıyla başlatıldı ve çalışıyor.");
            } catch (Exception e) {
                running.set(false);
                log("Başlatılırken hata oluştu: " + e.getMessage());
                throw new RuntimeException("Node başlatılamadı: " + nodeId, e);
            }
        } else {
            log("Zaten çalışıyor durumda.");
        }
    }

    /**
     * Düğümü durdurur. Abonelikleri ve NATS bağlantısını temizler.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log("Durduruluyor...");
            try {
                onStop();
            } catch (Exception e) {
                log("onStop() hatası: " + e.getMessage());
            }

            // JetStream aboneliklerini sonlandır
            for (Map.Entry<String, JetStreamSubscription> entry : streamSubscriptions.entrySet()) {
                try {
                    entry.getValue().unsubscribe();
                } catch (Exception ignored) {}
            }
            streamSubscriptions.clear();

            // Core NATS aboneliklerini sonlandır
            if (coreDispatcher != null) {
                for (Map.Entry<String, Subscription> entry : coreSubscriptions.entrySet()) {
                    try {
                        coreDispatcher.unsubscribe(entry.getValue());
                    } catch (Exception ignored) {}
                }
                coreSubscriptions.clear();
                try {
                    if (connection != null) {
                        connection.closeDispatcher(coreDispatcher);
                    }
                } catch (Exception ignored) {}
            }

            if (streamDispatcher != null && connection != null) {
                try {
                    connection.closeDispatcher(streamDispatcher);
                } catch (Exception ignored) {}
            }

            // NATS Bağlantısını kapat
            if (connection != null) {
                try {
                    connection.flush(Duration.ofSeconds(2));
                } catch (Exception ignored) {}
                try {
                    connection.close();
                    log("NATS bağlantısı güvenle kapatıldı.");
                } catch (Exception e) {
                    log("NATS bağlantısı kapatılırken hata: " + e.getMessage());
                }
            }
            log("Durduruldu.");
        }
    }

    /**
     * Alt sınıfların kendi başlatma ve abonelik mantığını yazacağı soyut metot.
     */
    protected abstract void onStart() throws Exception;

    /**
     * Alt sınıfların kaynak temizliğini yapacağı soyut metot.
     */
    protected abstract void onStop() throws Exception;

    // =========================================================================
    // BAĞLANTI (CONNECTION) YÖNETİMİ
    // =========================================================================

    /**
     * NATS sunucusuna bağlanır, JetStream bağlamlarını ve Dispatcher'ları hazırlar.
     */
    public synchronized void connect() throws IOException, InterruptedException {
        if (connection == null || connection.getStatus() != Connection.Status.CONNECTED) {
            log("NATS sunucusuna bağlanılıyor: " + natsUrl);
            Options options = buildNatsOptions();
            this.connection = Nats.connect(options);
            this.jetStream = connection.jetStream();
            this.jetStreamManagement = connection.jetStreamManagement();
            this.coreDispatcher = connection.createDispatcher();
            this.streamDispatcher = connection.createDispatcher();
            log("NATS Core ve JetStream bağlantısı kuruldu.");
        }
    }

    protected Options buildNatsOptions() {
        return new Options.Builder()
                .server(natsUrl)
                .connectionName(nodeId)
                .maxReconnects(-1) // sınırsız yeniden bağlanma denemesi
                .reconnectWait(Duration.ofSeconds(2))
                .connectionListener((conn, type) -> log("Bağlantı durumu: " + type))
                .errorListener(new ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        log("NATS Hata: " + error);
                    }
                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        log("NATS İstisna: " + exp.getMessage());
                    }
                })
                .build();
    }

    private void ensureConnected() {
        if (connection == null || connection.getStatus() != Connection.Status.CONNECTED) {
            throw new IllegalStateException("NATS bağlantısı aktif değil. Önce connect() veya start() çağrılmalıdır.");
        }
    }

    private void ensureJetStream() {
        ensureConnected();
        if (jetStream == null) {
            throw new IllegalStateException("JetStream bağlamı aktif değil.");
        }
    }

    // =========================================================================
    // JETSTREAM STREAM YÖNETİMİ
    // =========================================================================

    /**
     * JetStream üzerinde belirtilen stream mevcut değilse oluşturur.
     *
     * @param streamName Stream adı (örn. "STREAM_EVENTS")
     * @param storageType Bellek (Memory) veya Dosya (File) saklama
     * @param subjects Bu stream'in toplayacağı topic desenleri (örn. "sensor.*", "order.created")
     * @return StreamInfo nesnesi
     */
    public StreamInfo ensureStream(String streamName, StorageType storageType, String... subjects) throws IOException, JetStreamApiException {
        ensureJetStream();
        try {
            StreamInfo info = jetStreamManagement.getStreamInfo(streamName);
            log(String.format("Stream '%s' zaten mevcut. Subjects: %s", streamName, info.getConfiguration().getSubjects()));
            return info;
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 10059 || e.getMessage().toLowerCase().contains("stream not found")) {
                StreamConfiguration config = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subjects)
                        .storageType(storageType != null ? storageType : StorageType.File)
                        .build();
                StreamInfo created = jetStreamManagement.addStream(config);
                log(String.format("Yeni JetStream stream oluşturuldu: '%s', Subjects: %s", streamName, Arrays.toString(subjects)));
                return created;
            }
            throw e;
        }
    }

    public StreamInfo ensureStream(String streamName, String... subjects) throws IOException, JetStreamApiException {
        return ensureStream(streamName, StorageType.File, subjects);
    }

    // =========================================================================
    // 1. NATS CORE - PUBLISH (YAYINLAMA)
    // =========================================================================

    /**
     * NATS Core: Belirtilen topic'e byte array olarak mesaj yayınlar.
     */
    public void publishCore(String subject, byte[] data) {
        ensureConnected();
        connection.publish(subject, data);
    }

    /**
     * NATS Core: Belirtilen topic'e düz metin (String) mesaj yayınlar.
     */
    public void publishCore(String subject, String text) {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        publishCore(subject, data);
    }

    /**
     * NATS Core: Belirtilen nesneyi JSON'a serileştirerek yayınlar.
     */
    public void publishCoreJson(String subject, Object payload) {
        publishCore(subject, toJson(payload));
    }

    /**
     * NATS Core: İstek-Yanıt (Request-Reply) deseni için replyTo adresiyle yayınlar.
     */
    public void publishCore(String subject, String replyTo, byte[] data) {
        ensureConnected();
        connection.publish(subject, replyTo, data);
    }

    // =========================================================================
    // 2. NATS CORE - SUBSCRIBE (ABONELİK & HANDLER ATAMA)
    // =========================================================================

    /**
     * NATS Core: Belirtilen topic'i dinler ve gelen her mesaj için atanan handler'ı çalıştırır.
     * Farklı topic'ler için ayrı ayrı handler atanarak çağrılabilir.
     *
     * @param subject Dinlenecek konu (örn. "core.telemetry")
     * @param handler Mesajı işleyecek özel MessageHandler
     * @return Subscription nesnesi
     */
    public Subscription subscribeCore(String subject, MessageHandler handler) {
        ensureConnected();
        Subscription sub = coreDispatcher.subscribe(subject, handler);
        coreSubscriptions.put(subject, sub);
        log(String.format("Core NATS dinleniyor: '%s'", subject));
        return sub;
    }

    /**
     * NATS Core: Queue Group (yük dengeleme) desteği ile dinleme yapar.
     */
    public Subscription subscribeCore(String subject, String queueGroup, MessageHandler handler) {
        ensureConnected();
        Subscription sub = coreDispatcher.subscribe(subject, queueGroup, handler);
        coreSubscriptions.put(subject, sub);
        log(String.format("Core NATS dinleniyor (Queue: '%s'): '%s'", queueGroup, subject));
        return sub;
    }

    /**
     * NATS Core: Gelen mesajı doğrudan String formatında işleyen pratik dinleyici.
     */
    public Subscription subscribeCoreText(String subject, Consumer<String> textConsumer) {
        return subscribeCore(subject, msg -> {
            String text = new String(msg.getData(), StandardCharsets.UTF_8);
            textConsumer.accept(text);
        });
    }

    /**
     * NATS Core: Gelen JSON mesajını otomatik POJO tipine dönüştürerek işleyen dinleyici.
     */
    public <T> Subscription subscribeCoreJson(String subject, Class<T> clazz, Consumer<T> consumer) {
        return subscribeCore(subject, msg -> {
            try {
                T obj = fromJson(msg.getData(), clazz);
                consumer.accept(obj);
            } catch (Exception e) {
                log(String.format("Core JSON ayrıştırma hatası ('%s'): %s", subject, e.getMessage()));
            }
        });
    }

    /**
     * NATS Core aboneliğini iptal eder.
     */
    public void unsubscribeCore(String subject) {
        Subscription sub = coreSubscriptions.remove(subject);
        if (sub != null && coreDispatcher != null) {
            coreDispatcher.unsubscribe(sub);
            log(String.format("Core NATS aboneliği kaldırıldı: '%s'", subject));
        }
    }

    // =========================================================================
    // 3. JETSTREAM - PUBLISH (YAYINLAMA)
    // =========================================================================

    /**
     * JetStream: Senkron olarak byte array yayınlar (sunucu onay - PublishAck bekler).
     */
    public PublishAck publishStream(String subject, byte[] data) throws IOException, JetStreamApiException {
        ensureJetStream();
        return jetStream.publish(subject, data);
    }

    /**
     * JetStream: Senkron olarak düz metin (String) yayınlar.
     */
    public PublishAck publishStream(String subject, String text) throws IOException, JetStreamApiException {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return publishStream(subject, data);
    }

    /**
     * JetStream: Senkron olarak nesneyi JSON formatında yayınlar.
     */
    public PublishAck publishStreamJson(String subject, Object payload) throws IOException, JetStreamApiException {
        return publishStream(subject, toJson(payload));
    }

    /**
     * JetStream: Asenkron olarak byte array yayınlar (CompletableFuture döner).
     */
    public CompletableFuture<PublishAck> publishStreamAsync(String subject, byte[] data) {
        ensureJetStream();
        return jetStream.publishAsync(subject, data);
    }

    /**
     * JetStream: Asenkron olarak String yayınlar.
     */
    public CompletableFuture<PublishAck> publishStreamAsync(String subject, String text) {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return publishStreamAsync(subject, data);
    }

    /**
     * JetStream: Asenkron olarak nesneyi JSON formatında yayınlar.
     */
    public CompletableFuture<PublishAck> publishStreamJsonAsync(String subject, Object payload) {
        return publishStreamAsync(subject, toJson(payload));
    }

    // =========================================================================
    // 4. JETSTREAM - SUBSCRIBE (ABONELİK & HANDLER ATAMA)
    // =========================================================================

    /**
     * JetStream: Belirtilen stream topic'ini dinler ve atanan MessageHandler'ı çalıştırır.
     *
     * @param subject Dinlenecek stream subject'i
     * @param handler Gelen mesajları işleyen MessageHandler
     * @param autoAck Otomatik ACK yapılsın mı (false ise handler içinde msg.ack() çağrılmalıdır)
     * @return JetStreamSubscription nesnesi
     */
    public JetStreamSubscription subscribeStream(String subject, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, streamDispatcher, handler, autoAck);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (autoAck=%b): '%s'", autoAck, subject));
        return sub;
    }

    /**
     * JetStream: PushSubscribeOptions seçenekleri (durable consumer vb.) ile dinler.
     */
    public JetStreamSubscription subscribeStream(String subject, PushSubscribeOptions options, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, streamDispatcher, handler, autoAck, options);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (PushOptions, autoAck=%b): '%s'", autoAck, subject));
        return sub;
    }

    /**
     * JetStream: Queue Group ile dinler (birden fazla node arasında stream mesajlarını paylaştırır).
     */
    public JetStreamSubscription subscribeStream(String subject, String queueGroup, PushSubscribeOptions options, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, queueGroup, streamDispatcher, handler, autoAck, options);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (Queue: '%s', autoAck=%b): '%s'", queueGroup, autoAck, subject));
        return sub;
    }

    /**
     * JetStream: Gelen JSON mesajını otomatik POJO tipine deserialize edip StreamItemHandler'a iletir.
     *
     * @param subject Dinlenecek topic
     * @param clazz Deserialize edilecek POJO sınıfı
     * @param handler Tiplendirilmiş nesne ve orijinal Message'ı alan handler
     * @param autoAck Otomatik ACK durumu
     */
    public <T> JetStreamSubscription subscribeStreamJson(String subject, Class<T> clazz, StreamItemHandler<T> handler, boolean autoAck) throws IOException, JetStreamApiException {
        return subscribeStream(subject, msg -> {
            try {
                T obj = fromJson(msg.getData(), clazz);
                handler.handle(obj, msg);
            } catch (Exception e) {
                log(String.format("JetStream JSON ayrıştırma hatası ('%s'): %s", subject, e.getMessage()));
                if (!autoAck) {
                    msg.nak();
                }
            }
        }, autoAck);
    }

    /**
     * JetStream aboneliğini iptal eder.
     */
    public void unsubscribeStream(String subject) {
        JetStreamSubscription sub = streamSubscriptions.remove(subject);
        if (sub != null) {
            try {
                sub.unsubscribe();
                log(String.format("JetStream aboneliği kaldırıldı: '%s'", subject));
            } catch (Exception e) {
                log(String.format("JetStream aboneliği kaldırılırken hata: '%s' - %s", subject, e.getMessage()));
            }
        }
    }

    // =========================================================================
    // YARDIMCI METOTLAR (JSON, GETTER, SETTER)
    // =========================================================================

    public Gson getGson() {
        return gson;
    }

    public String toJson(Object obj) {
        return gson.toJson(obj);
    }

    public <T> T fromJson(String json, Class<T> clazz) {
        return gson.fromJson(json, clazz);
    }

    public <T> T fromJson(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) return null;
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), clazz);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNatsUrl() {
        return natsUrl;
    }

    public void setNatsUrl(String natsUrl) {
        this.natsUrl = natsUrl;
    }

    public boolean isAutoConnect() {
        return autoConnect;
    }

    public void setAutoConnect(boolean autoConnect) {
        this.autoConnect = autoConnect;
    }

    public boolean isRunning() {
        return running.get();
    }

    public Connection getConnection() {
        return connection;
    }

    public JetStream getJetStream() {
        return jetStream;
    }

    public JetStreamManagement getJetStreamManagement() {
        return jetStreamManagement;
    }

    /**
     * Standart loglama formatı: [nodeId] mesaj
     */
    protected void log(String message) {
        System.out.printf("[%s] %s%n", nodeId, message);
    }
}
