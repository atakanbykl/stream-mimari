package com.stream.base;

import com.google.gson.Gson;
import com.stream.base.config.NodeConfig;
import io.nats.client.*;
import io.nats.client.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Tüm node-* servislerinin extend edeceği temel soyut sınıf.
 * 
 * Bu sınıf alt sınıflara şunları sağlar:
 * 1. NATS Core: Ultra düşük gecikmeli anlık mesajlaşma (in-memory pub/sub).
 * 2. JetStream: Uygulama geçmiş hafızası (temporal memory/state store).
 * 3. Doğrudan Geçmiş Sorgulama (Direct Get): İstediği an geçmişteki son mesajı çekme (getLastMessage).
 * 4. Canlı Dinleme (subscribeStreamLive): Geçmişteki mesajları tekrar etmeden yalnızca yeni olayları dinleme.
 * 5. Otomatik Stream Yönetimi: allowDirect=true ve RetentionPolicy.Limits ile güvenli kalıcılık.
 * 6. Topic Ad Alanı Kalkanı (Validation): core.> ve stream.> konularının karışmasını önleme.
 * 7. Gson tabanlı otomatik JSON serileştirme / deserileştirme.
 * 8. start() ve stop() ile thread-safe yaşam döngüsü ve kaynak temizliği.
 * 9. Merkezi ve hiyerarşik konfigürasyon yönetimi (NodeConfig).
 */
public abstract class BaseNode {

    public static final String DEFAULT_STREAM_NAME = "STREAM_MIMARI";
    public static final String DEFAULT_STREAM_SUBJECT = "stream.>";

    protected final String nodeId;
    protected final NodeConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String natsUrl = "nats://127.0.0.1:4222";
    private boolean autoConnect = true;
    private boolean strictTopicValidation = false;

    // NATS ve JetStream nesneleri
    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jetStreamManagement;
    private Dispatcher coreDispatcher;
    private Dispatcher streamDispatcher;

    // Aktif abonelik kayıtları
    private final Map<String, Subscription> coreSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, JetStreamSubscription> streamSubscriptions = new ConcurrentHashMap<>();

    // Bilinen stream isimleri ve eşleşen topic önbelleği
    private final Set<String> knownStreams = ConcurrentHashMap.newKeySet();
    private final Map<String, String> subjectToStreamMap = new ConcurrentHashMap<>();

    // JSON serileştirici
    private final Gson gson = new Gson();

    public BaseNode(String nodeId) {
        this(nodeId, (NodeConfig) null);
    }

    public BaseNode(String nodeId, String natsUrl) {
        this.nodeId = nodeId;
        this.config = NodeConfig.load(nodeId);
        this.natsUrl = (natsUrl != null && !natsUrl.isBlank()) ? natsUrl : config.getString(NodeConfig.NATS_URL, "nats://127.0.0.1:4222");
    }

    public BaseNode(String nodeId, NodeConfig config) {
        this.nodeId = nodeId;
        this.config = (config != null) ? config : NodeConfig.load(nodeId);
        this.natsUrl = this.config.getString(NodeConfig.NATS_URL, "nats://127.0.0.1:4222");
    }

    public NodeConfig getConfig() {
        return config;
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
                log("Başarıyla başlatıldı ve dinlemede.");
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
     * Düğümü durdurur. Tüm Core/Stream aboneliklerini ve NATS bağlantısını temizler.
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
                    if (streamDispatcher != null) {
                        streamDispatcher.unsubscribe(entry.getValue());
                    } else {
                        entry.getValue().unsubscribe();
                    }
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

    protected abstract void onStart() throws Exception;
    protected abstract void onStop() throws Exception;

    // =========================================================================
    // BAĞLANTI (CONNECTION) YÖNETİMİ
    // =========================================================================

    public synchronized void connect() throws IOException, InterruptedException {
        if (connection == null || connection.getStatus() != Connection.Status.CONNECTED) {
            log("NATS sunucusuna bağlanılıyor: " + natsUrl);
            Options options = buildNatsOptions();
            this.connection = Nats.connect(options);
            this.jetStream = connection.jetStream();
            this.jetStreamManagement = connection.jetStreamManagement();
            this.coreDispatcher = connection.createDispatcher();
            this.streamDispatcher = connection.createDispatcher();
            log("NATS Core ve JetStream bağlamı başarıyla hazırlandı.");
        }
    }

    protected Options buildNatsOptions() {
        int maxReconnects = config.getInt(NodeConfig.NATS_MAX_RECONNECTS, -1);
        int reconnectWaitSec = config.getInt(NodeConfig.NATS_RECONNECT_WAIT_SEC, 2);
        String connName = config.getString(NodeConfig.NATS_CONNECTION_NAME, nodeId);

        return new Options.Builder()
                .server(natsUrl)
                .connectionName(connName)
                .maxReconnects(maxReconnects)
                .reconnectWait(Duration.ofSeconds(reconnectWaitSec))
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

    protected void ensureConnected() {
        if (connection == null || connection.getStatus() != Connection.Status.CONNECTED) {
            throw new IllegalStateException("NATS bağlantısı aktif değil. Önce connect() veya start() çağrılmalıdır.");
        }
    }

    protected void ensureJetStream() {
        ensureConnected();
        if (jetStream == null) {
            throw new IllegalStateException("JetStream bağlamı aktif değil.");
        }
    }

    // =========================================================================
    // TOPIC AD ALANI KALKANI VE DOĞRULAMA (VALIDATION)
    // =========================================================================

    /**
     * NATS Core konuları için ad alanı doğrulamasını yapar.
     * Core konuları 'core.' ile başlamalıdır. 'stream.' ile başlıyorsa uyarı/hata verir.
     */
    public void validateCoreSubject(String subject) {
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("Subject boş olamaz.");
        }
        if (subject.startsWith("stream.")) {
            String warn = String.format("MİMARİ KURAL İHLALİ: '%s' konusu JetStream öneki (stream.) içeriyor fakat NATS Core üzerinden işlem görüyor!", subject);
            log(warn);
            if (strictTopicValidation) {
                throw new IllegalArgumentException(warn);
            }
        } else if (!subject.startsWith("core.")) {
            log(String.format("[BİLGİ] '%s' konusu 'core.' standart öneki ile başlamıyor.", subject));
        }
    }

    /**
     * JetStream konuları için ad alanı doğrulamasını yapar.
     * JetStream konuları 'stream.' ile başlamalıdır. 'core.' ile başlıyorsa uyarı/hata verir.
     */
    public void validateStreamSubject(String subject) {
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("Subject boş olamaz.");
        }
        if (subject.startsWith("core.")) {
            String warn = String.format("MİMARİ KURAL İHLALİ: '%s' konusu NATS Core öneki (core.) içeriyor fakat JetStream üzerinden işlem görüyor!", subject);
            log(warn);
            if (strictTopicValidation) {
                throw new IllegalArgumentException(warn);
            }
        } else if (!subject.startsWith("stream.")) {
            log(String.format("[BİLGİ] '%s' konusu 'stream.' standart öneki ile başlamıyor.", subject));
        }
    }

    // =========================================================================
    // STREAM TANIMLAMA VE YÖNETİMİ
    // =========================================================================

    /**
     * Tüm 'stream.>' konularını kapsayan varsayılan mimari stream'ini (STREAM_MIMARI) oluşturur veya doğrular.
     */
    public StreamInfo ensureDefaultStream() throws IOException, JetStreamApiException {
        String streamName = config.getString(NodeConfig.NATS_STREAM_NAME, DEFAULT_STREAM_NAME);
        String subject = config.getString(NodeConfig.NATS_STREAM_SUBJECTS, DEFAULT_STREAM_SUBJECT);
        String storage = config.getString(NodeConfig.NATS_STREAM_STORAGE, "file");
        StorageType storageType = "memory".equalsIgnoreCase(storage) ? StorageType.Memory : StorageType.File;
        return ensureStream(streamName, storageType, subject);
    }

    /**
     * JetStream üzerinde belirtilen stream'in varlığından emin olur.
     * Geçmiş hafızasının kesinlikle bozulmaması için:
     * - RetentionPolicy.Limits (Subscribers mesajları ACK verse bile stream'den silinmez)
     * - allowDirect = true (İstenen an doğrudan ve tahribatsız Direct Get sorgusu)
     * - denyDelete = true (Mesajların dışarıdan veya koddan tekil silinmesi engellenir)
     * - denyPurge = true (Stream'in tamamen temizlenmesi/silinmesi engellenir)
     * - discardPolicy = DiscardPolicy.Old (Kapasite durumunda FIFO kuralı)
     * - maxMessagesPerSubject = -1 (Yeni yayınlar eski mesajların üzerine yazılmaz, tüm geçmiş korunur)
     *
     * Mevcut bir stream bulunursa bu koruma standartları updateStream ile otomatik uygulanır.
     *
     * @param streamName Stream adı
     * @param storageType File veya Memory saklama
     * @param subjects Bu stream'in kapsayacağı topic kalıpları
     */
    public StreamInfo ensureStream(String streamName, StorageType storageType, String... subjects) throws IOException, JetStreamApiException {
        ensureJetStream();
        try {
            StreamInfo info = jetStreamManagement.getStreamInfo(streamName);
            knownStreams.add(streamName);
            for (String sub : subjects) {
                subjectToStreamMap.put(sub, streamName);
            }

            // Mevcut stream üzerinde kalıcı hafıza koruma standartlarını doğrula ve güncelle
            StreamConfiguration curr = info.getConfiguration();
            if (curr != null) {
                boolean needsUpdate = false;
                StreamConfiguration.Builder updateBuilder = StreamConfiguration.builder(curr);

                if (!curr.getAllowDirect()) {
                    updateBuilder.allowDirect(true);
                    needsUpdate = true;
                }
                if (curr.getRetentionPolicy() != RetentionPolicy.Limits) {
                    log(String.format("⚠️ [KRİTİK GÜNCELLEME] '%s' stream'i RetentionPolicy.Limits olarak güncelleniyor (Geçmiş hafızasının korunması için)!", streamName));
                    updateBuilder.retentionPolicy(RetentionPolicy.Limits);
                    needsUpdate = true;
                }
                if (!curr.getDenyDelete()) {
                    updateBuilder.denyDelete(true);
                    needsUpdate = true;
                }
                if (!curr.getDenyPurge()) {
                    updateBuilder.denyPurge(true);
                    needsUpdate = true;
                }
                if (curr.getMaxMsgsPerSubject() != -1) {
                    updateBuilder.maxMessagesPerSubject(-1);
                    needsUpdate = true;
                }
                if (curr.getDiscardPolicy() != DiscardPolicy.Old) {
                    updateBuilder.discardPolicy(DiscardPolicy.Old);
                    needsUpdate = true;
                }

                // Konu kalıplarını birleştir
                List<String> existingSubs = curr.getSubjects() != null ? curr.getSubjects() : Collections.emptyList();
                Set<String> mergedSubs = new LinkedHashSet<>(existingSubs);
                for (String sub : subjects) {
                    if (mergedSubs.add(sub)) {
                        needsUpdate = true;
                    }
                }
                if (needsUpdate) {
                    updateBuilder.subjects(new ArrayList<>(mergedSubs));
                    info = jetStreamManagement.updateStream(updateBuilder.build());
                    log(String.format("Stream '%s' kalıcı geçmiş hafızası koruma standartlarına güncellendi (Limits, allowDirect=true, denyDelete=true, denyPurge=true).", streamName));
                }
            }

            log(String.format("Stream '%s' kalıcı hafızası hazır ve koruma altında. Subjects: %s", streamName, info.getConfiguration().getSubjects()));
            return info;
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 10059 || e.getMessage().toLowerCase().contains("stream not found")) {
                StreamConfiguration config = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subjects)
                        .storageType(storageType != null ? storageType : StorageType.File)
                        .retentionPolicy(RetentionPolicy.Limits) // Pub/Sub mesajları ASLA silmez
                        .discardPolicy(DiscardPolicy.Old)
                        .allowDirect(true) // Doğrudan son mesajı çekebilmek (Direct Get) için ZORUNLU
                        .denyDelete(true) // Geçmiş mesajların tekil silinmesini engeller
                        .denyPurge(true) // Stream'in tamamen temizlenmesini engeller
                        .maxMessagesPerSubject(-1) // Konu bazlı eski mesajları silmez, tüm geçmişi tutar
                        .build();
                StreamInfo created = jetStreamManagement.addStream(config);
                knownStreams.add(streamName);
                for (String sub : subjects) {
                    subjectToStreamMap.put(sub, streamName);
                }
                log(String.format("Yeni JetStream stream oluşturuldu (Kalıcı Geçmiş Hafızası: Limits, allowDirect=true, denyDelete=true, denyPurge=true): '%s', Subjects: %s",
                        streamName, Arrays.toString(subjects)));
                return created;
            }
            throw e;
        }
    }

    public StreamInfo ensureStream(String streamName, String... subjects) throws IOException, JetStreamApiException {
        return ensureStream(streamName, StorageType.File, subjects);
    }

    /**
     * Verilen subject'in ait olduğu stream adını belirler.
     * 'stream.' ile başlayan konular varsayılan STREAM_MIMARI stream'ine doğrudan çözümlenir.
     */
    public String resolveStreamName(String subject) {
        if (subject == null) {
            return DEFAULT_STREAM_NAME;
        }
        if (subjectToStreamMap.containsKey(subject)) {
            return subjectToStreamMap.get(subject);
        }
        if (subject.startsWith("stream.") || knownStreams.contains(DEFAULT_STREAM_NAME)) {
            return DEFAULT_STREAM_NAME;
        }
        try {
            if (jetStreamManagement != null) {
                List<String> matching = jetStreamManagement.getStreamNames(subject);
                if (matching != null && !matching.isEmpty()) {
                    String found = matching.get(0);
                    subjectToStreamMap.put(subject, found);
                    return found;
                }
            }
        } catch (Exception ignored) {}
        return DEFAULT_STREAM_NAME;
    }

    // =========================================================================
    // 1. NATS CORE - PUBLISH & SUBSCRIBE (ULTRA DÜŞÜK GECİKME)
    // =========================================================================

    public boolean isConnected() {
        return connection != null && connection.getStatus() == Connection.Status.CONNECTED;
    }

    public void publishCore(String subject, byte[] data) {
        validateCoreSubject(subject);
        if (!isConnected()) {
            log(String.format("[UYARI] NATS bağlantısı aktif değil, '%s' konusuna mesaj gönderilemedi.", subject));
            return;
        }
        connection.publish(subject, data);
    }

    public void publishCore(String subject, String text) {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        publishCore(subject, data);
    }

    public void publishCoreJson(String subject, Object payload) {
        publishCore(subject, toJson(payload));
    }

    public void publishCore(String subject, String replyTo, byte[] data) {
        validateCoreSubject(subject);
        if (!isConnected()) {
            log(String.format("[UYARI] NATS bağlantısı aktif değil, '%s' konusuna yanıt gönderilemedi.", subject));
            return;
        }
        connection.publish(subject, replyTo, data);
    }

    public Subscription subscribeCore(String subject, MessageHandler handler) {
        validateCoreSubject(subject);
        ensureConnected();
        Subscription sub = coreDispatcher.subscribe(subject, handler);
        coreSubscriptions.put(subject, sub);
        log(String.format("Core NATS dinleniyor: '%s'", subject));
        return sub;
    }

    public Subscription subscribeCore(String subject, String queueGroup, MessageHandler handler) {
        validateCoreSubject(subject);
        ensureConnected();
        Subscription sub = coreDispatcher.subscribe(subject, queueGroup, handler);
        coreSubscriptions.put(subject, sub);
        log(String.format("Core NATS dinleniyor (Queue: '%s'): '%s'", queueGroup, subject));
        return sub;
    }

    public Subscription subscribeCoreText(String subject, Consumer<String> textConsumer) {
        return subscribeCore(subject, msg -> {
            String text = new String(msg.getData(), StandardCharsets.UTF_8);
            textConsumer.accept(text);
        });
    }

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

    public void unsubscribeCore(String subject) {
        Subscription sub = coreSubscriptions.remove(subject);
        if (sub != null && coreDispatcher != null) {
            coreDispatcher.unsubscribe(sub);
            log(String.format("Core NATS aboneliği kaldırıldı: '%s'", subject));
        }
    }

    // =========================================================================
    // 2. JETSTREAM - PUBLISH (KALICI GEÇMİŞ HAFIZASINA YAYIN)
    // =========================================================================

    public PublishAck publishStream(String subject, byte[] data) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        return jetStream.publish(subject, data);
    }

    public PublishAck publishStream(String subject, String text) throws IOException, JetStreamApiException {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return publishStream(subject, data);
    }

    public PublishAck publishStreamJson(String subject, Object payload) throws IOException, JetStreamApiException {
        return publishStream(subject, toJson(payload));
    }

    public CompletableFuture<PublishAck> publishStreamAsync(String subject, byte[] data) {
        validateStreamSubject(subject);
        ensureJetStream();
        return jetStream.publishAsync(subject, data);
    }

    public CompletableFuture<PublishAck> publishStreamAsync(String subject, String text) {
        byte[] data = (text != null) ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return publishStreamAsync(subject, data);
    }

    public CompletableFuture<PublishAck> publishStreamJsonAsync(String subject, Object payload) {
        return publishStreamAsync(subject, toJson(payload));
    }

    // =========================================================================
    // 3. JETSTREAM - GEÇMİŞTEN SON MESAJI ÇEKME (LAST-BY-SUBJECT / DIRECT GET)
    // =========================================================================

    /**
     * Belirtilen stream ve topic için geçmişteki en son kaydedilen mesajı çeker.
     * Konuda henüz hiç mesaj yoksa (cold start) Optional.empty() döner, çökmez.
     *
     * @param streamName Stream adı
     * @param subject İlgili konu
     * @return MessageInfo sarmalayıcısı (varsa)
     */
    public Optional<MessageInfo> getLastMessage(String streamName, String subject) {
        validateStreamSubject(subject);
        if (!isConnected() || jetStreamManagement == null) {
            return Optional.empty();
        }
        try {
            MessageInfo info = jetStreamManagement.getLastMessage(streamName, subject);
            if (info != null && info.hasError()) {
                return Optional.empty();
            }
            return Optional.ofNullable(info);
        } catch (JetStreamApiException e) {
            // 10037: no message found (konu henüz boş)
            // 10059: stream not found
            if (e.getErrorCode() == 10037 || e.getErrorCode() == 10059 ||
                    e.getMessage().toLowerCase().contains("no message found")) {
                return Optional.empty();
            }
            log(String.format("getLastMessage hatası ('%s'/'%s'): %s", streamName, subject, e.getMessage()));
            return Optional.empty();
        } catch (Exception e) {
            log(String.format("getLastMessage bağlantı hatası ('%s'/'%s'): %s", streamName, subject, e.getMessage()));
            return Optional.empty();
        }
    }

    /**
     * Stream adını otomatik çözümleyerek konunun geçmişteki son mesajını çeker.
     */
    public Optional<MessageInfo> getLastMessage(String subject) {
        String streamName = resolveStreamName(subject);
        return getLastMessage(streamName, subject);
    }

    /**
     * Konunun geçmişteki son mesajını düz metin (String) olarak çeker.
     */
    public Optional<String> getLastMessageText(String subject) {
        return getLastMessage(subject).map(msg -> new String(msg.getData(), StandardCharsets.UTF_8));
    }

    /**
     * Konunun geçmişteki son mesajını JSON'dan deserialize ederek Java nesnesi olarak çeker.
     */
    public <T> Optional<T> getLastMessageJson(String subject, Class<T> clazz) {
        return getLastMessage(subject).map(msg -> fromJson(msg.getData(), clazz));
    }

    /**
     * Konunun geçmişteki son mesajını payload + sequence + timestamp meta verileriyle birlikte döner.
     */
    public <T> Optional<StreamRecord<T>> getLastMessageRecord(String subject, Class<T> clazz) {
        return getLastMessage(subject).map(msg -> {
            T payload = fromJson(msg.getData(), clazz);
            return new StreamRecord<>(payload, msg.getSeq(), msg.getTime(), msg.getSubject(), msg.getHeaders());
        });
    }

    /**
     * Belirtilen stream ve sıra numarası (sequence) için geçmişteki mesajı doğrudan çeker.
     * Bu işlem Direct Get kullandığı için veriyi stream'den KESİNLİKLE SİLMEZ ve değiştirmez.
     *
     * @param streamName Stream adı
     * @param sequence Sıra numarası (1, 2, ...)
     * @return MessageInfo sarmalayıcısı (varsa)
     */
    public Optional<MessageInfo> getMessageBySequence(String streamName, long sequence) {
        if (!isConnected() || jetStreamManagement == null) {
            return Optional.empty();
        }
        try {
            MessageInfo info = jetStreamManagement.getMessage(streamName, sequence);
            if (info != null && info.hasError()) {
                return Optional.empty();
            }
            return Optional.ofNullable(info);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 10037 || e.getMessage().toLowerCase().contains("no message found")) {
                return Optional.empty();
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<MessageInfo> getMessageBySequence(long sequence) {
        String defaultStream = config.getString(NodeConfig.NATS_STREAM_NAME, DEFAULT_STREAM_NAME);
        return getMessageBySequence(defaultStream, sequence);
    }

    public Optional<String> getMessageTextBySequence(long sequence) {
        return getMessageBySequence(sequence).map(msg -> new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public <T> Optional<T> getMessageJsonBySequence(long sequence, Class<T> clazz) {
        return getMessageBySequence(sequence).map(msg -> fromJson(msg.getData(), clazz));
    }

    public <T> Optional<StreamRecord<T>> getMessageRecordBySequence(long sequence, Class<T> clazz) {
        return getMessageBySequence(sequence).map(msg -> {
            T payload = fromJson(msg.getData(), clazz);
            return new StreamRecord<>(payload, msg.getSeq(), msg.getTime(), msg.getSubject(), msg.getHeaders());
        });
    }

    /**
     * Belirtilen konu (subject) için stream'e kaydedilmiş ilk (en eski) mesajı çeker.
     * Direct Get kullandığı için veriyi silmez, stream hafızasını bozmaz.
     */
    public Optional<MessageInfo> getFirstMessage(String streamName, String subject) {
        validateStreamSubject(subject);
        if (!isConnected() || jetStreamManagement == null) {
            return Optional.empty();
        }
        try {
            MessageInfo info = jetStreamManagement.getFirstMessage(streamName, subject);
            if (info != null && info.hasError()) {
                return Optional.empty();
            }
            return Optional.ofNullable(info);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<MessageInfo> getFirstMessage(String subject) {
        String streamName = resolveStreamName(subject);
        return getFirstMessage(streamName, subject);
    }

    public Optional<String> getFirstMessageText(String subject) {
        return getFirstMessage(subject).map(msg -> new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public <T> Optional<T> getFirstMessageJson(String subject, Class<T> clazz) {
        return getFirstMessage(subject).map(msg -> fromJson(msg.getData(), clazz));
    }

    public <T> Optional<StreamRecord<T>> getFirstMessageRecord(String subject, Class<T> clazz) {
        return getFirstMessage(subject).map(msg -> {
            T payload = fromJson(msg.getData(), clazz);
            return new StreamRecord<>(payload, msg.getSeq(), msg.getTime(), msg.getSubject(), msg.getHeaders());
        });
    }

    /**
     * Belirtilen konu için verilen sıra numarasından (sequence) sonraki mesajı çeker.
     */
    public Optional<MessageInfo> getNextMessage(String streamName, long sequence, String subject) {
        validateStreamSubject(subject);
        if (!isConnected() || jetStreamManagement == null) {
            return Optional.empty();
        }
        try {
            MessageInfo info = jetStreamManagement.getNextMessage(streamName, sequence, subject);
            if (info != null && info.hasError()) {
                return Optional.empty();
            }
            return Optional.ofNullable(info);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<MessageInfo> getNextMessage(long sequence, String subject) {
        String streamName = resolveStreamName(subject);
        return getNextMessage(streamName, sequence, subject);
    }

    public <T> Optional<StreamRecord<T>> getNextMessageRecord(long sequence, String subject, Class<T> clazz) {
        return getNextMessage(sequence, subject).map(msg -> {
            T payload = fromJson(msg.getData(), clazz);
            return new StreamRecord<>(payload, msg.getSeq(), msg.getTime(), msg.getSubject(), msg.getHeaders());
        });
    }

    /**
     * Belirtilen konu (subject) için geçmişteki tüm mesajları (veya limit adede kadar)
     * Direct Get kullanarak stream'den liste olarak çeker.
     * 
     * BU İŞLEM STREAM HAFIZASINI ASLA BOZMAZ VEYA DEĞİŞTİRMEZ.
     * Çekilen tüm mesajlar stream içinde kalmaya devam eder ve başka düğümler
     * veya sorgular tarafından tekrar tekrar çekilebilir.
     *
     * @param subject İlgili konu
     * @param clazz Model sınıfı
     * @param limit Maksimum getirilecek kayıt sayısı (0 veya negatif ise sınırsız)
     * @return Kronolojik sıralı geçmiş kayıt listesi
     */
    public <T> List<StreamRecord<T>> getHistoricalRecords(String subject, Class<T> clazz, int limit) {
        validateStreamSubject(subject);
        List<StreamRecord<T>> records = new ArrayList<>();
        if (!isConnected() || jetStreamManagement == null) {
            return records;
        }

        String streamName = resolveStreamName(subject);
        Optional<MessageInfo> currentOpt = getFirstMessage(streamName, subject);
        int count = 0;

        while (currentOpt.isPresent() && (limit <= 0 || count < limit)) {
            MessageInfo msg = currentOpt.get();
            try {
                T payload = fromJson(msg.getData(), clazz);
                records.add(new StreamRecord<>(payload, msg.getSeq(), msg.getTime(), msg.getSubject(), msg.getHeaders()));
            } catch (Exception e) {
                log(String.format("Geçmiş mesaj ayrıştırma hatası [Sıra: %d]: %s", msg.getSeq(), e.getMessage()));
            }
            count++;

            // Bir sonraki mesajı sorgula (sequence + 1'den itibaren)
            currentOpt = getNextMessage(streamName, msg.getSeq() + 1, subject);
        }
        return records;
    }

    public <T> List<StreamRecord<T>> getHistoricalRecords(String subject, Class<T> clazz) {
        return getHistoricalRecords(subject, clazz, 0);
    }

    // =========================================================================
    // 4. JETSTREAM - ANLIK VE GEÇMİŞ ABONELİKLER (SUBSCRIBE)
    // =========================================================================

    /**
     * JetStream CANLI Dinleme: Yalnızca bu andan sonra gelen YENİ mesajları anlık dinler (DeliverPolicy.New).
     * Eski geçmiş mesajları tüketiciye tekrar akıtmaz.
     */
    public JetStreamSubscription subscribeStreamLive(String subject, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .deliverPolicy(DeliverPolicy.New)
                .build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .configuration(cc)
                .build();
        return subscribeStream(subject, pso, handler, autoAck);
    }

    /**
     * JetStream CANLI JSON Dinleme: Yalnızca yeni mesajları JSON'dan dönüştürerek dinler.
     */
    public <T> JetStreamSubscription subscribeStreamLiveJson(String subject, Class<T> clazz, StreamItemHandler<T> handler, boolean autoAck) throws IOException, JetStreamApiException {
        return subscribeStreamLive(subject, msg -> {
            try {
                T obj = fromJson(msg.getData(), clazz);
                handler.handle(obj, msg);
            } catch (Exception e) {
                log(String.format("Canlı Stream JSON hatası ('%s'): %s", subject, e.getMessage()));
                if (!autoAck) {
                    msg.nak();
                }
            }
        }, autoAck);
    }

    /**
     * JetStream GEÇMİŞTEN YENİDEN OYNATMA (REPLAY):
     * Konunun geçmişteki TÜM mesajlarını en baştan itibaren (DeliverPolicy.All) dinler
     * ve ardından canlı gelenleri almaya devam eder.
     * 
     * RetentionPolicy.Limits sayesinde mesajların okunması ve ACK verilmesi
     * stream hafızasından mesajları KESİNLİKLE SİLMEZ. Başka tüketiciler de bağımsız olarak okuyabilir.
     */
    public JetStreamSubscription subscribeStreamReplay(String subject, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .deliverPolicy(DeliverPolicy.All)
                .build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .configuration(cc)
                .build();
        return subscribeStream(subject, pso, handler, autoAck);
    }

    public <T> JetStreamSubscription subscribeStreamReplayJson(String subject, Class<T> clazz, StreamItemHandler<T> handler, boolean autoAck) throws IOException, JetStreamApiException {
        return subscribeStreamReplay(subject, msg -> {
            try {
                T obj = fromJson(msg.getData(), clazz);
                handler.handle(obj, msg);
            } catch (Exception e) {
                log(String.format("Stream Replay JSON hatası ('%s'): %s", subject, e.getMessage()));
                if (!autoAck) {
                    msg.nak();
                }
            }
        }, autoAck);
    }

    /**
     * JetStream BELİRLİ BİR SIRA NUMARASINDAN İTİBAREN DİNLEME:
     * Belirtilen sequence numarasından başlayarak (DeliverPolicy.ByStartSequence) geçmişi ve canlıyı dinler.
     */
    public JetStreamSubscription subscribeStreamFromSequence(String subject, long startSequence, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .startSequence(startSequence)
                .build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .configuration(cc)
                .build();
        return subscribeStream(subject, pso, handler, autoAck);
    }

    public <T> JetStreamSubscription subscribeStreamFromSequenceJson(String subject, long startSequence, Class<T> clazz, StreamItemHandler<T> handler, boolean autoAck) throws IOException, JetStreamApiException {
        return subscribeStreamFromSequence(subject, startSequence, msg -> {
            try {
                T obj = fromJson(msg.getData(), clazz);
                handler.handle(obj, msg);
            } catch (Exception e) {
                log(String.format("Stream FromSequence JSON hatası ('%s'): %s", subject, e.getMessage()));
                if (!autoAck) {
                    msg.nak();
                }
            }
        }, autoAck);
    }

    /**
     * Standart JetStream dinleme.
     */
    public JetStreamSubscription subscribeStream(String subject, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, streamDispatcher, handler, autoAck);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (autoAck=%b): '%s'", autoAck, subject));
        return sub;
    }

    public JetStreamSubscription subscribeStream(String subject, PushSubscribeOptions options, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, streamDispatcher, handler, autoAck, options);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (PushOptions, autoAck=%b): '%s'", autoAck, subject));
        return sub;
    }

    public JetStreamSubscription subscribeStream(String subject, String queueGroup, PushSubscribeOptions options, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateStreamSubject(subject);
        ensureJetStream();
        JetStreamSubscription sub = jetStream.subscribe(subject, queueGroup, streamDispatcher, handler, autoAck, options);
        streamSubscriptions.put(subject, sub);
        log(String.format("JetStream dinleniyor (Queue: '%s', autoAck=%b): '%s'", queueGroup, autoAck, subject));
        return sub;
    }

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

    public void unsubscribeStream(String subject) {
        JetStreamSubscription sub = streamSubscriptions.remove(subject);
        if (sub != null) {
            unsubscribeStream(sub);
            log(String.format("JetStream aboneliği kaldırıldı: '%s'", subject));
        }
    }

    public void unsubscribeStream(JetStreamSubscription sub) {
        if (sub != null) {
            try {
                if (streamDispatcher != null) {
                    streamDispatcher.unsubscribe(sub);
                } else {
                    sub.unsubscribe();
                }
            } catch (Exception e) {
                log("JetStream aboneliği kaldırılırken hata: " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // YARDIMCI METOTLAR
    // =========================================================================

    public boolean isStrictTopicValidation() {
        return strictTopicValidation;
    }

    public void setStrictTopicValidation(boolean strictTopicValidation) {
        this.strictTopicValidation = strictTopicValidation;
    }

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

    protected void log(String message) {
        System.out.printf("[%s] %s%n", nodeId, message);
    }
}
