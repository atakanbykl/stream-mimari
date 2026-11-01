package com.stream.node1;

/**
 * node-1 üzerinde entegre edilen TopicAggregator mekanizmasını
 * basitçe konsolda gösteren çalıştırılabilir Demo sınıfı.
 */
public class AggregatorDemoRunner {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=========================================================================");
        System.out.println("🚀 ÇOKLU TOPIC BİRLEŞTİRİCİ (TopicAggregator) BASİT DEMO");
        System.out.println("=========================================================================");

        App app = new App();

        // ---------------------------------------------------------------------
        // SENARYO 1: 4 TOPIC ARKA ARKAYA GELİR -> TEK SEFERDE YAYINLANIR
        // ---------------------------------------------------------------------
        System.out.println("\n▶ SENARYO 1: 4 farklı topic arka arkaya gönderiliyor...");
        long before1 = app.getDemoAggregator().getFlushCount();
        app.simulateFullSetBurstDemo();
        long after1 = app.getDemoAggregator().getFlushCount();
        System.out.printf("✅ SONUÇ 1: 4 girdi mesajı için çıkan birleşik yayın sayısı = %d%n", (after1 - before1));

        // ---------------------------------------------------------------------
        // SENARYO 2: T1 ÜST ÜSTE 3 KEZ GELİR -> ÖNCEKİLER EZİLMEDEN HEMEN ÇIKAR
        // ---------------------------------------------------------------------
        System.out.println("\n▶ SENARYO 2: Diğerleri gelmeden T1 (Komut) üst üste 3 kez gönderiliyor...");
        long before2 = app.getDemoAggregator().getFlushCount();
        app.simulateCollisionBurstDemo();
        long rightAfterCollision = app.getDemoAggregator().getFlushCount();
        System.out.printf("⚡ Çakışma nedeniyle anında çıkan yayın sayısı = %d (İlk 2 T1 korundu!)%n",
                (rightAfterCollision - before2));

        System.out.println("⏳ 3. T1 için pencere süresi (250ms) bekleniyor...");
        Thread.sleep(350);
        System.out.printf("⏱️ Timeout sonrası toplam yayın sayısı = %d%n",
                app.getDemoAggregator().getFlushCount());

        app.stop();
        System.out.println("\n🎉 DEMO TAMAMLANDI.");
    }
}
