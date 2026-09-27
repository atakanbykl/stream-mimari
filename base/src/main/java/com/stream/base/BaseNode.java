package com.stream.base;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tüm node-* servislerinin extend edeceği temel soyut sınıf.
 * Yaşam döngüsü (start, stop), NATS bağlantı bilgisi ve ortak loglama mekanizmasını içerir.
 */
public abstract class BaseNode {

    private final String nodeId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String natsUrl = "nats://127.0.0.1:4222";

    public BaseNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public BaseNode(String nodeId, String natsUrl) {
        this.nodeId = nodeId;
        this.natsUrl = natsUrl;
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

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Düğümü başlatır ve alt sınıftaki onStart() metodunu çağırır.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log("Başlatılıyor...");
            try {
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
     * Düğümü durdurur ve alt sınıftaki onStop() metodunu çağırır.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log("Durduruluyor...");
            try {
                onStop();
                log("Durduruldu.");
            } catch (Exception e) {
                log("Durdurulurken hata oluştu: " + e.getMessage());
            }
        }
    }

    /**
     * Alt sınıfların kendi başlatma ve abone olma mantıklarını yazacağı metot.
     * @throws Exception Başlatma hatası
     */
    protected abstract void onStart() throws Exception;

    /**
     * Alt sınıfların kaynakları serbest bırakacağı durdurma metodu.
     * @throws Exception Durdurma hatası
     */
    protected abstract void onStop() throws Exception;

    /**
     * Standart konsol loglama formatı: [nodeId] mesaj
     * @param message Log mesajı
     */
    protected void log(String message) {
        System.out.printf("[%s] %s%n", nodeId, message);
    }
}
