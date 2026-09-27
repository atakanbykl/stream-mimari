package com.stream.base;

import io.nats.client.Message;

/**
 * JetStream üzerinden gelen JSON veya tiplendirilmiş verileri işlemek için fonksiyonel arayüz.
 *
 * @param <T> Mesaj içeriğinin deserialize edileceği sınıf tipi
 */
@FunctionalInterface
public interface StreamItemHandler<T> {

    /**
     * Gelen mesajı işler.
     *
     * @param payload Deserialize edilmiş Java nesnesi
     * @param message Orijinal NATS Message nesnesi (manuel ACK gerekiyorsa message.ack() yapılabilir)
     * @throws Exception İşleme sırasında fırlatılabilecek istisnalar
     */
    void handle(T payload, Message message) throws Exception;
}
