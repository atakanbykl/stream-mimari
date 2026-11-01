package com.stream.base;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basitleştirilmiş TopicAggregator birim testleri.
 */
public class TopicAggregatorTest {

    @Test
    @DisplayName("1. 4 Topic Arka Arkaya Geldiğinde 4/4 Bariyer Dolmalı ve Timeout Beklemeden Tek Sefer Yayın Çıkmalı")
    public void testFullSetBarrierFlushesImmediatelyOnce() {
        List<Map<String, Object>> published = new CopyOnWriteArrayList<>();
        AtomicReference<TopicAggregator> ref = new AtomicReference<>();

        try (TopicAggregator agg = new TopicAggregator(2000, () -> published.add(ref.get().getAll()),
                "t1", "t2", "t3", "t4")) {
            ref.set(agg);

            agg.update("t1", "Deger-1");
            agg.update("t2", "Deger-2");
            agg.update("t3", "Deger-3");
            assertEquals(0, published.size(), "3/4 geldiğinde henüz yayın çıkmamalı.");

            // 4. topic geldiği anda timeout beklemeden tek seferde çıkmalı
            agg.update("t4", "Deger-4");
            assertEquals(1, published.size(), "4/4 tamamlandığı anda 1 kez yayın çıkmalıdır!");
            assertEquals("Deger-1", published.get(0).get("t1"));
            assertEquals("Deger-4", published.get(0).get("t4"));
        }
    }

    @Test
    @DisplayName("2. T1 Üst Üste Geldiğinde (1. T1 -> 2. T1 -> 3. T1) Önceki Veriler Ezilmeden İletilmeli")
    public void testRepeatedCollisionsOnTopics() {
        List<Map<String, Object>> published = new CopyOnWriteArrayList<>();
        AtomicReference<TopicAggregator> ref = new AtomicReference<>();

        try (TopicAggregator agg = new TopicAggregator(2000, () -> published.add(ref.get().getAll()),
                "t1", "t2", "t3", "t4")) {
            ref.set(agg);

            // 1. T1("v1") gelir -> diğer dörtlüyü bekler
            agg.update("t1", "v1");
            assertEquals(0, published.size());

            // 2. T1("v2") gelir -> önceki "v1" ezilmeden hemen yayınlanır, "v2" yeni dörtlüyü bekler
            agg.update("t1", "v2");
            assertEquals(1, published.size());
            assertEquals("v1", published.get(0).get("t1"), "İlk yayında önceki 'v1' korunmalıdır!");

            // 3. T1("v3") gelir -> önceki "v2" ezilmeden hemen yayınlanır, "v3" yeni dörtlüyü bekler
            agg.update("t1", "v3");
            assertEquals(2, published.size());
            assertEquals("v2", published.get(1).get("t1"), "İkinci yayında önceki 'v2' korunmalıdır!");

            // Şimdi diğer t2, t3, t4 geldiğinde "v3" ile birlikte 4/4 set olarak yayınlanır
            agg.update("t2", "b2");
            agg.update("t3", "c3");
            agg.update("t4", "d4");
            assertEquals(3, published.size());
            assertEquals("v3", published.get(2).get("t1"));
            assertEquals("d4", published.get(2).get("t4"));
        }
    }

    @Test
    @DisplayName("3. Sadece Kısmi Güncelleme Geldiğinde Timeout Sonunda Yayın Çıkmalı")
    public void testPartialUpdateFlushesOnTimeout() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        try (TopicAggregator agg = new TopicAggregator(60, latch::countDown, "t1", "t2", "t3", "t4")) {
            agg.update("t1", 100);
            agg.update("t2", 200);
            assertEquals(0L, agg.getFlushCount());

            assertTrue(latch.await(1, TimeUnit.SECONDS), "Timeout sonunda yayın çıkmalıdır.");
            assertEquals(1L, agg.getFlushCount());
        }
    }

    @Test
    @DisplayName("4. 2 ile Sonsuz (N=2 ve N=10) Aralığında Esnek Topic Sayısı")
    public void testFlexibleTopicCount() {
        List<String> tenTopics = new ArrayList<>();
        for (int i = 1; i <= 10; i++) tenTopics.add("s" + i);

        try (TopicAggregator agg2 = new TopicAggregator(1000, () -> {}, "a", "b");
             TopicAggregator agg10 = new TopicAggregator(1000, () -> {}, tenTopics)) {

            agg2.update("a", 1);
            assertEquals(0L, agg2.getFlushCount());
            agg2.update("b", 2);
            assertEquals(1L, agg2.getFlushCount(), "2 topic tamamlandığında yayın çıkmalı.");

            for (int i = 1; i <= 9; i++) agg10.update("s" + i, i);
            assertEquals(0L, agg10.getFlushCount());
            agg10.update("s10", 10);
            assertEquals(1L, agg10.getFlushCount(), "10 topic tamamlandığında yayın çıkmalı.");
        }
    }
}
