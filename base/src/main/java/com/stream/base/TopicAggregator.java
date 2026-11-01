package com.stream.base;

import java.util.*;
import java.util.concurrent.*;

/**
 * 2 ile sonsuz (N >= 2) sayıda farklı topic'ten gelen verileri birleştirip
 * tek bir yayın tetiklemek için kullanılan basit ve yeniden kullanılabilir sınıf.
 *
 * Çalışma Kuralları:
 * 1. Çakışma (Collision): Aktif pencerede zaten gelmiş bir topic tekrar gelirse,
 *    yeni değer yazılmadan ÖNCE mevcut pencere hemen yayınlanır (flush).
 * 2. Tam Set (N/N Bariyer): Beklenen tüm topic'lerden veri geldiği anda beklemeden yayınlanır.
 * 3. Timeout: Tüm topic'ler gelmezse timeoutMs sonunda mevcut verilerle yayınlanır.
 */
public class TopicAggregator implements AutoCloseable {

    private final Set<String> expectedTopics = new LinkedHashSet<>();
    private final Set<String> pendingTopics = new LinkedHashSet<>();
    private final Map<String, Object> latestValues = new ConcurrentHashMap<>();

    private final long timeoutMs;
    private final Runnable onFlush;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timeoutFuture;
    private long flushCount = 0;
    private boolean closed = false;

    public TopicAggregator(long timeoutMs, Runnable onFlush, String... topics) {
        this(timeoutMs, onFlush, Arrays.asList(topics));
    }

    public TopicAggregator(long timeoutMs, Runnable onFlush, Collection<String> topics) {
        if (topics == null || topics.size() < 2) {
            throw new IllegalArgumentException("En az 2 topic verilmelidir.");
        }
        this.expectedTopics.addAll(topics);
        this.timeoutMs = timeoutMs;
        this.onFlush = Objects.requireNonNull(onFlush);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "topic-aggregator");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * İlgili topic'ten yeni veri geldiğinde çağrılır (değeri dahili haritada saklar).
     */
    public synchronized void update(String topic, Object newValue) {
        update(topic, newValue, null);
    }

    /**
     * İlgili topic'ten yeni veri geldiğinde çağrılır (özel alan atamasını çalıştırır).
     */
    public synchronized void update(String topic, Runnable stateSetter) {
        update(topic, null, stateSetter);
    }

    /**
     * Ana güncelleme metodu:
     * 1. Aynı topic bu pencerede zaten geldiyse önce eski veriyi yayınlar (flush).
     * 2. Yeni veriyi kaydeder.
     * 3. Tüm topic'ler tamamlandıysa (N/N) hemen yayınlar; değilse timeout başlatır.
     */
    public synchronized void update(String topic, Object newValue, Runnable stateSetter) {
        if (closed || topic == null) return;

        expectedTopics.add(topic);

        // 1. ÇAKIŞMA KONTROLÜ: Aynı topic tekrar geldiyse önceki veriyi ezmeden hemen yayınla
        if (pendingTopics.contains(topic)) {
            flush();
        }

        // 2. Yeni veriyi kaydet ve aktif pencereye ekle
        if (newValue != null) {
            latestValues.put(topic, newValue);
        }
        if (stateSetter != null) {
            stateSetter.run();
        }
        pendingTopics.add(topic);

        // 3. TAM SET BARİYERİ (N/N): Hepsi geldiyse beklemeden yayınla
        if (pendingTopics.containsAll(expectedTopics)) {
            flush();
            return;
        }

        // 4. Eksik topic varsa zamanlayıcıyı başlat/yenile
        resetTimer();
    }

    private synchronized void flush() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
        if (pendingTopics.isEmpty()) return;

        pendingTopics.clear();
        flushCount++;
        onFlush.run();
    }

    private void resetTimer() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        if (!closed) {
            timeoutFuture = scheduler.schedule(this::flush, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String topic) {
        return (T) latestValues.get(topic);
    }

    public Map<String, Object> getAll() {
        return new LinkedHashMap<>(latestValues);
    }

    public synchronized long getFlushCount() {
        return flushCount;
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        pendingTopics.clear();
        scheduler.shutdownNow();
    }
}
