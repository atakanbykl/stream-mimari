package com.stream.base;

import com.stream.base.model.VehicleStatus;
import io.nats.client.*;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.*;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Stream Geçmiş Hafızası Bütünlük ve Tahribatsızlık (Immutability & Non-Destructive) Testi.
 * 
 * Bu test sınıfı kullanıcının en kritik mimari şartlarını doğrular:
 * 1. Stream geçmişin hafızasıdır: JetStream RetentionPolicy.Limits, allowDirect=true, denyDelete=true, denyPurge=true.
 * 2. Yayınlanan her yeni mesaj kronolojik olarak eklenir, eski mesajların üzerine YAZILMAZ (maxMessagesPerSubject = -1).
 * 3. Tüketiciler (subscribers) mesajları okuyup ACK verse dahi mesajlar stream'den KESİNLİKLE SİLİNMEZ.
 * 4. Herhangi bir anda bir yerden geçmiş veri (Direct Get) istendiğinde çekilir ve veri stream'de durmaya DEVAM EDER.
 * 5. Başka bir tüketici/istemci herhangi bir zamanda o veriyi TEKRAR çekebilir (çoklu bağımsız okuyucu garantisi).
 * 6. getHistoricalRecords ile konunun tüm geçmişi kronolojik liste olarak çekilebilir.
 * 7. subscribeStreamReplay ile yeni bir istemci geçmişteki tüm verileri en baştan itibaren yeniden oynatabilir.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StreamMemoryIntegrityTest {

    private static final String NATS_URL = "nats://127.0.0.1:4222";
    private static final String TEST_STREAM = "STREAM_MIMARI";
    private static final String TEST_SUBJECT = "stream.test.memory.status";

    private static boolean isNatsAvailable = false;
    private static TestNode nodeA;
    private static TestNode nodeB;

    static class TestNode extends BaseNode {
        public TestNode(String nodeId) {
            super(nodeId, NATS_URL);
        }

        @Override
        protected void onStart() throws Exception {
            ensureDefaultStream();
        }

        @Override
        protected void onStop() throws Exception {
        }
    }

    @BeforeAll
    public static void checkNatsAndSetup() {
        try (Socket socket = new Socket("127.0.0.1", 4222)) {
            isNatsAvailable = true;
        } catch (Exception e) {
            isNatsAvailable = false;
        }

        if (isNatsAvailable) {
            nodeA = new TestNode("reader-node-A");
            nodeB = new TestNode("reader-node-B");
            nodeA.start();
            nodeB.start();
        }
    }

    @AfterAll
    public static void tearDown() {
        if (nodeA != null) nodeA.stop();
        if (nodeB != null) nodeB.stop();
    }

    @Test
    @Order(1)
    @DisplayName("1. Stream Konfigürasyonunun Geçmiş Hafızası Korumalarını Doğrula")
    public void testStreamConfigurationImmutabilityGuarantees() throws Exception {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        StreamInfo info = nodeA.getJetStreamManagement().getStreamInfo(TEST_STREAM);
        assertNotNull(info);
        assertNotNull(info.getConfiguration());

        // Zorunlu Koruma Standartları:
        assertEquals(RetentionPolicy.Limits, info.getConfiguration().getRetentionPolicy(),
                "RetentionPolicy mutlaka Limits olmalıdır. (WorkQueue veya Interest mesajları tüketildikten sonra siler!)");
        assertTrue(info.getConfiguration().getAllowDirect(),
                "allowDirect true olmalıdır. (Doğrudan geçmiş veriyi çekmek için zorunludur.)");
        assertTrue(info.getConfiguration().getDenyDelete(),
                "denyDelete true olmalıdır. (Mesajların tekil olarak silinmesini engeller.)");
        assertTrue(info.getConfiguration().getDenyPurge(),
                "denyPurge true olmalıdır. (Stream hafızasının tamamen temizlenmesini engeller.)");
        assertEquals(-1, info.getConfiguration().getMaxMsgsPerSubject(),
                "maxMsgsPerSubject -1 (sınırsız) olmalıdır. (Yeni yayın eski mesajı ezmemelidir.)");
        assertEquals(DiscardPolicy.Old, info.getConfiguration().getDiscardPolicy(),
                "discardPolicy Old olmalıdır. (FIFO event log standardı.)");
    }

    @Test
    @Order(2)
    @DisplayName("2. Yayınlanan Mesajların Kronolojik Sırayla Birikmesi ve Sıra Numaraları")
    public void testAppendOnlyPublishIncrementsSequence() throws Exception {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        // 3 farklı araç durum mesajı yayınlayalım
        VehicleStatus status1 = new VehicleStatus(90.0, 95.0, 1000.0, 85.0);
        VehicleStatus status2 = new VehicleStatus(85.0, 90.0, 1010.0, 87.0);
        VehicleStatus status3 = new VehicleStatus(80.0, 85.0, 1020.0, 89.0);

        PublishAck ack1 = nodeA.publishStreamJson(TEST_SUBJECT, status1);
        PublishAck ack2 = nodeA.publishStreamJson(TEST_SUBJECT, status2);
        PublishAck ack3 = nodeA.publishStreamJson(TEST_SUBJECT, status3);

        assertTrue(ack1.getSeqno() < ack2.getSeqno(), "Sequence numaraları monoton artmalıdır.");
        assertTrue(ack2.getSeqno() < ack3.getSeqno(), "Sequence numaraları monoton artmalıdır.");
    }

    @Test
    @Order(3)
    @DisplayName("3. Bağımsız İki Okuyucunun (Node A ve Node B) Aynı Geçmiş Veriyi Çekebilmesi")
    public void testMultipleIndependentReadersDirectGetWithoutMutation() {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        // Node A son mesajı çeker
        Optional<StreamRecord<VehicleStatus>> lastA = nodeA.getLastMessageRecord(TEST_SUBJECT, VehicleStatus.class);
        assertTrue(lastA.isPresent(), "Node A son geçmiş kaydını bulabilmelidir.");
        assertEquals(80.0, lastA.get().getPayload().fuelPercent);

        // Node B (farklı bir istemci) son mesajı çeker
        Optional<StreamRecord<VehicleStatus>> lastB = nodeB.getLastMessageRecord(TEST_SUBJECT, VehicleStatus.class);
        assertTrue(lastB.isPresent(), "Node B aynı geçmiş kaydını bulabilmelidir.");
        assertEquals(80.0, lastB.get().getPayload().fuelPercent);

        // İki okuyucunun gördüğü sıra numarası ve zaman damgası birebir aynı olmalı
        assertEquals(lastA.get().getSequence(), lastB.get().getSequence(), "Sıra numaraları aynı olmalıdır.");
        assertEquals(lastA.get().getTimestamp(), lastB.get().getTimestamp(), "Zaman damgaları aynı olmalıdır.");

        // Node A tekrar çeker - Veri hala oradadır!
        Optional<StreamRecord<VehicleStatus>> lastA2 = nodeA.getLastMessageRecord(TEST_SUBJECT, VehicleStatus.class);
        assertTrue(lastA2.isPresent());
        assertEquals(lastA.get().getSequence(), lastA2.get().getSequence(), "Veri çekilse de stream'de kalmaya devam eder.");
    }

    @Test
    @Order(4)
    @DisplayName("4. getHistoricalRecords ile Tüm Geçmiş Kronolojisinin Liste Olarak Alınması")
    public void testGetHistoricalRecordsReturnsAllPastMessages() {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        // Node A konunun tüm geçmişini liste olarak çeker
        List<StreamRecord<VehicleStatus>> historyA = nodeA.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class);
        assertTrue(historyA.size() >= 3, "En az 3 adet geçmiş kayıt dönmelidir.");

        // Kronolojik sıralı olmalı
        for (int i = 1; i < historyA.size(); i++) {
            assertTrue(historyA.get(i).getSequence() > historyA.get(i - 1).getSequence(),
                    "Geçmiş kayıtlar sıra numarasına göre küçükten büyüğe gelmelidir.");
        }

        // Node B de tüm geçmişi çeker - Hiçbir kayıt eksilmemiştir
        List<StreamRecord<VehicleStatus>> historyB = nodeB.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class);
        assertEquals(historyA.size(), historyB.size(), "Node B de aynı sayıda geçmiş kayıt görmelidir.");
    }

    @Test
    @Order(5)
    @DisplayName("5. Tüketici (Subscriber) Mesajı Dinleyip ACK Verse Bile Stream'den Silinmemesi")
    public void testSubscriberAckDoesNotDeleteMessageFromStream() throws Exception {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        long countBefore = nodeA.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class).size();

        // Yeni bir durum yayınlayalım
        VehicleStatus newStatus = new VehicleStatus(77.7, 88.8, 1050.0, 91.0);
        PublishAck ack = nodeA.publishStreamJson(TEST_SUBJECT, newStatus);
        long targetSeq = ack.getSeqno();

        // Bir subscriber açalım ve mesajı alıp explicitly ACK edelim
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);

        JetStreamSubscription sub = nodeA.subscribeStream(
                TEST_SUBJECT,
                msg -> {
                    receivedCount.incrementAndGet();
                    msg.ack(); // Tüketici mesajı ACK eder
                    latch.countDown();
                },
                false // manual ACK
        );

        boolean received = latch.await(3, TimeUnit.SECONDS);
        assertTrue(received, "Subscriber mesajı teslim almalı ve ACK vermelidir.");
        nodeA.unsubscribeStream(sub);

        // KRİTİK KONTROL: Tüketici ACK vermiş olmasına rağmen hedef mesaj stream'de hala duruyor mu?
        Optional<StreamRecord<VehicleStatus>> msgAfterAck = nodeB.getMessageRecordBySequence(targetSeq, VehicleStatus.class);
        assertTrue(msgAfterAck.isPresent(), "Tüketici ACK vermiş olsa bile mesaj stream içinde SAPASAĞLAM durmalıdır!");
        assertEquals(77.7, msgAfterAck.get().getPayload().fuelPercent);

        // getHistoricalRecords ile kontrol: toplam kayıt sayısı azalmamış olmalı
        long countAfter = nodeA.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class).size();
        assertEquals(countBefore + 1, countAfter, "Tüketici ACK'i stream kayıt sayısını KESİNLİKLE azaltmamalıdır.");
    }

    @Test
    @Order(6)
    @DisplayName("6. Geçmişi Yeniden Oynatma (subscribeStreamReplay - DeliverPolicy.All)")
    public void testStreamReplayReceivesAllPastMessagesWithoutDeleting() throws Exception {
        assumeTrue(isNatsAvailable, "NATS sunucusu aktif değil, test atlandı.");

        List<StreamRecord<VehicleStatus>> expectedRecords = nodeA.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class);
        int expectedCount = expectedRecords.size();
        assertTrue(expectedCount >= 4);

        CountDownLatch replayLatch = new CountDownLatch(expectedCount);
        List<Long> replayedSeqs = new ArrayList<>();

        JetStreamSubscription replaySub = nodeB.subscribeStreamReplayJson(
                TEST_SUBJECT,
                VehicleStatus.class,
                (status, msg) -> {
                    replayedSeqs.add(msg.metaData().streamSequence());
                    msg.ack();
                    replayLatch.countDown();
                },
                false
        );

        boolean completed = replayLatch.await(5, TimeUnit.SECONDS);
        nodeB.unsubscribeStream(replaySub);

        assertTrue(completed, "Replay abonesi geçmişteki tüm mesajları teslim almalıdır.");
        assertEquals(expectedCount, replayedSeqs.size());

        // Replay sonrası verilerin hala stream'de durduğunu son kez doğrula
        List<StreamRecord<VehicleStatus>> finalCheck = nodeA.getHistoricalRecords(TEST_SUBJECT, VehicleStatus.class);
        assertEquals(expectedCount, finalCheck.size(), "Tüm replay operasyonundan sonra dahi hafıza eksiksiz korunmalıdır.");
    }
}
