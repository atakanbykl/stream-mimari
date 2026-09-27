package com.stream.base;

import io.nats.client.impl.Headers;
import java.time.ZonedDateTime;

/**
 * JetStream geçmiş hafızasından çekilen mesajların gövdesiyle (payload) birlikte
 * meta verilerini (sıra no, zaman damgası, konu vb.) taşıyan sarmalayıcı sınıf.
 *
 * @param <T> Deserialize edilmiş nesnenin tipi
 */
public class StreamRecord<T> {

    private final T payload;
    private final long sequence;
    private final ZonedDateTime timestamp;
    private final String subject;
    private final Headers headers;

    public StreamRecord(T payload, long sequence, ZonedDateTime timestamp, String subject, Headers headers) {
        this.payload = payload;
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.subject = subject;
        this.headers = headers;
    }

    public T getPayload() {
        return payload;
    }

    public long getSequence() {
        return sequence;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public String getSubject() {
        return subject;
    }

    public Headers getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return "StreamRecord{" +
                "subject='" + subject + '\'' +
                ", sequence=" + sequence +
                ", timestamp=" + timestamp +
                ", payload=" + payload +
                '}';
    }
}

