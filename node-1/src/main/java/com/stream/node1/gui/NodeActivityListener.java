package com.stream.node1.gui;

/**
 * node-1 Olay Dinleyici Arayüzü (Event Listener)
 * 
 * node-1'in NATS Core ve JetStream üzerinden anlık aldığı, attığı
 * ve geçmişten (Direct Get) çektiği mesajları GUI'ye bildirmek için kullanılır.
 */
public interface NodeActivityListener {

    /**
     * NATS Core veya JetStream üzerinden yeni bir mesaj alındığında tetiklenir.
     */
    void onMessageReceived(String topic, String layer, String content);

    /**
     * NATS Core veya JetStream üzerinden yeni bir mesaj yayınlandığında tetiklenir.
     */
    void onMessageSent(String topic, String layer, String content);

    /**
     * JetStream geçmiş hafızasından bir mesaj doğrudan çekildiğinde tetiklenir.
     */
    void onHistoricalRetrieved(String topic, long sequence, String timestamp, String content);

    /**
     * Aracın hız, devir, vites vb. dinamik durumu değiştiğinde tetiklenir.
     */
    void onStateChanged();
}
