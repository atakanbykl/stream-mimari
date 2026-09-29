package com.stream.base;

import io.nats.client.api.MessageInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;

import java.time.Instant;
import java.time.ZonedDateTime;

/**
 * Direct Get (getLastMessage, getMessageBySequence) ile JetStream geçmiş hafızasından
 * çekilen mesajların standart io.nats.client.Message ve NatsMessage olarak sunulmasını sağlayan sınıf.
 * 
 * Standart bir NatsMessage nesnesinde metaData() çağrıldığında "Message is not a JetStream message"
 * istisnası fırlatılırken ve mesajın sunucuya iletilme zamanı (timestamp) ile sıra numarası (sequence)
 * kaybolurken; bu sınıf tüm JetStream meta verilerini eksiksiz muhafaza eder:
 * 
 * 1. msg.getData() -> Orijinal mesaj verisi (byte[])
 * 2. msg.getSubject() -> Mesajın yayınlandığı konu
 * 3. msg.getHeaders() -> Orijinal başlıklar + Nats-Time-Stamp, Nats-Sequence, Nats-Stream
 * 4. msg.isJetStream() -> true döner.
 * 5. msg.metaData().timestamp() -> Mesajın sunucuya kaydedilme zamanı (ZonedDateTime)
 * 6. msg.metaData().streamSequence() -> Mesajın stream içindeki sıra numarası (sequence)
 * 7. msg.metaData().getStream() -> Stream adı
 * 8. msg.ack() / msg.nak() -> Direct Get mesajı olduğu için güvenle NO-OP yapar.
 */
public class JetStreamDirectMessage extends NatsMessage {

    private final MessageInfo messageInfo;
    private NatsJetStreamMetaData metaData;

    public JetStreamDirectMessage(MessageInfo info) {
        super(
                info.getSubject(),
                buildSyntheticReplyTo(info),
                enrichHeaders(info),
                info.getData() != null ? info.getData() : new byte[0]
        );
        this.messageInfo = info;
    }

    private static String buildSyntheticReplyTo(MessageInfo info) {
        String stream = (info.getStream() != null && !info.getStream().isEmpty()) ? info.getStream() : "STREAM";
        long nanos = 0L;
        if (info.getTime() != null) {
            Instant instant = info.getTime().toInstant();
            nanos = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        }
        return "$JS.ACK." + stream + ".DIRECT_GET.1." + info.getSeq() + ".1." + nanos;
    }

    private static Headers enrichHeaders(MessageInfo info) {
        Headers headers = (info.getHeaders() != null) ? new Headers(info.getHeaders()) : new Headers();
        if (info.getTime() != null && !headers.containsKey("Nats-Time-Stamp")) {
            headers.add("Nats-Time-Stamp", info.getTime().toString());
        }
        if (!headers.containsKey("Nats-Sequence")) {
            headers.add("Nats-Sequence", String.valueOf(info.getSeq()));
        }
        if (info.getStream() != null && !headers.containsKey("Nats-Stream")) {
            headers.add("Nats-Stream", info.getStream());
        }
        return headers;
    }

    @Override
    public boolean isJetStream() {
        return true;
    }

    @Override
    public NatsJetStreamMetaData metaData() {
        if (metaData == null) {
            metaData = new NatsJetStreamMetaData(this);
        }
        return metaData;
    }

    /**
     * Orijinal MessageInfo nesnesini döner.
     */
    public MessageInfo getMessageInfo() {
        return messageInfo;
    }

    /**
     * Mesajın JetStream sunucusuna ulaştığı / kaydedildiği kesin zaman damgasını döner.
     */
    public ZonedDateTime getTimestamp() {
        return messageInfo != null ? messageInfo.getTime() : null;
    }

    /**
     * Mesajın stream içindeki sıra numarasını döner.
     */
    public long getSequence() {
        return messageInfo != null ? messageInfo.getSeq() : 0L;
    }
}
