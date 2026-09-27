# Stream Mimari Proje Kuralları (Architecture & Coding Guidelines)

Bu kurallar `stream-mimari` projesinde çalışan yapay zeka ajanı ve geliştiriciler için zorunlu mimari yönergelerdir.

## 1. Mimari Hiyerarşi ve Modül Yapısı
- Proje Maven çoklu modül (multi-module) yapısındadır.
- Kök dizindeki `pom.xml` (`packaging: pom`), tüm alt modülleri (`<modules>`) yönetir ve ortak kütüphane sürümlerini belirler.
- Yeni bir node servisi (`node-2`, `node-3`, vb.) eklendiğinde:
  1. Kök `pom.xml` dosyasına `<module>node-X</module>` olarak eklenmelidir.
  2. Yeni modülün `pom.xml` dosyasında parent olarak kök `stream-mimari` tanımlanmalı ve `<dependency>` olarak `base` modülü eklenmelidir.

## 2. BaseNode Genişletme (Inheritance) Zorunluluğu
- Tüm `node-*` projelerinin ana çalışma sınıfları mutlaka `com.stream.base.BaseNode` sınıfını `extend` etmelidir.
- Alt sınıflar doğrudan NATS bağlantısı oluşturmaya çalışmamalı; `BaseNode` tarafından sağlanan yaşam döngüsü (`start()`, `stop()`, `onStart()`, `onStop()`) ve metotları kullanmalıdır.
- Thread-safe kapatma için ana sınıflarda `Runtime.getRuntime().addShutdownHook(...)` kullanılmalıdır.

## 3. Çift Katmanlı Mesajlaşma ve Kalıcı Geçmiş Hafızası Standartları (Immutable Event Memory)
- **NATS Core (Anlık / Minimum Gecikme):**
  - Topic öneki: Mutlaka `core.<kategori>.<olay>` olmalıdır.
  - Disk persistansı yoktur; telemetri, acil sinyal ve hızlı RPC komutları için kullanılır.
- **JetStream (Sistem Kalıcı Geçmiş Hafızası & State Store):**
  - Topic öneki: Mutlaka `stream.<kategori>.<olay>` olmalıdır.
  - **Kesin Kalıcılık ve Değişmezlik (Immutability & Non-Destructive):**
    - Stream tanımlarında `RetentionPolicy.Limits`, `allowDirect = true`, `denyDelete = true`, `denyPurge = true`, `discardPolicy = DiscardPolicy.Old` ve `maxMessagesPerSubject = -1` ZORUNLUDUR (`BaseNode.ensureStream` bunu yeni ve mevcut tüm stream'lerde otomatik garanti eder).
    - **Pub & Sub Asla Hafızayı Bozamaz:** Yayıncıların yeni mesaj atması veya tüketicilerin (subscribers) mesajları ACK'lemesi mesajları stream'den KESİNLİKLE SİLMEZ. Her mesaj sıra numarasıyla (sequence) kalıcı olarak saklanır.
    - **Tahribatsız Geçmiş Sorgulama (Direct Get):** Herhangi bir anda bir veya birden fazla istemci veriyi doğrudan bellek/diskten çekebilir (`getLastMessageRecord`, `getMessageRecordBySequence`, `getFirstMessageRecord`, `getNextMessageRecord`, `getHistoricalRecords`). Veri çekildiğinde stream'de durmaya devam eder; başka bir istemci veya aynı istemci dilediği an o veriyi tekrar çekebilir.
    - **Canlı Dinleme vs. Yeniden Oynatma (Replay):**
      - Yalnızca yeni olaylar için: `subscribeStreamLive` (`DeliverPolicy.New`).
      - Geçmişi baştan sona yeniden dinlemek için: `subscribeStreamReplay` (`DeliverPolicy.All`) veya `subscribeStreamFromSequence` (`DeliverPolicy.ByStartSequence`).
- **Onaylama (ACK) Politikası:**
  - `autoAck = false` ile açılan JetStream aboneliklerinde mesaj işlendikten sonra mutlaka `msg.ack()` çağrılmalı; hata durumunda `msg.nak()` ile yeniden kuyruğa alınmalıdır. ACK vermek tüketiciye özel bir durumdur, stream'deki geçmiş veriyi KESİNLİKLE silmez.

## 4. Serileştirme, JSON ve Ortak Model Kütüphanesi
- Düğümler arası mesajlaşmada kullanılan tüm ortak veri modelleri/DTO'lar tek bir düğüme hapsedilmemeli, `base` modülü altındaki `com.stream.base.model` paketinde toplanmalıdır. Böylece tüm `node-*` servisleri `import com.stream.base.model.*;` ile aynı tip güvenli sözleşmeleri paylaşır.
- JSON dönüşümleri için `BaseNode` içerisindeki dahili `toJson(...)` ve `fromJson(...)` metotları kullanılmalıdır.
- Tip güvenli abonelikler için `subscribeCoreJson(...)`, `subscribeStreamLiveJson(...)` ve `getLastMessageJson(...)` tercih edilmelidir.

## 5. Antigravity Özelleştirmeleri (Self-Maintaining Customizations)
- Projede yeni bir iş akışı, kural, araç veya entegrasyon ihtiyacı doğduğunda ilgili özelleştirme proaktif olarak güncellenmeli veya yenisi oluşturulmalıdır:
  - **Beceriler (Skills):** Tekrarlanabilir karmaşık iş akışları, iskeleleme (scaffolding) veya operasyonlar için `.agents/skills/<ad>/SKILL.md` güncellenmeli/oluşturulmalıdır.
  - **Kurallar (Rules):** Kodlama ve mimari standartları güncellendiğinde `GEMINI.md` veya `.agents/rules/` dosyaları güncellenmelidir.
  - **Kancalar (Hooks):** İşlemler öncesi/sonrası otomatik doğrulama, derleme veya biçimlendirme gerektiğinde `.agents/hooks.json` tanımlanmalıdır.
  - **MCP:** NATS CLI, Kafka veya harici servis araçları bağlandığında `.agents/mcp_config.json` güncellenmelidir.
  - **Hafıza Dosyası (Memory Log):** Projede yapılan mimari değişiklikler, eklenen özellikler ve yol haritası `MEMORY.md` dosyasında güncel tutulmalıdır.

## 6. Dinamik Dış Konfigürasyon ve Katı Doğrulama Standartları (NodeConfig)
- Hiçbir `node-*` servisinde NATS URL'i, Stream adı, Topic isimleri, yayın aralıkları veya eşik değerleri koda **hardcoded** gömülmemelidir.
- Yapılandırmalar JAR içine gömülmemeli; derleme gerektirmeksizin düzenlenebilmesi için modül çalışma dizinindeki harici `application.properties` dosyasından okunmalıdır.
- Çalışma zamanında (runtime) konfigürasyon dosyası bulunamazsa, okunamazsa veya parametreler eksik/hatalı/mimari kurallara (`core.` / `stream.`) aykırıysa, `ConfigErrorHandler` aracılığıyla kullanıcıya modal bir hata pop-up'ı (`JOptionPane`) gösterilmeli ve kullanıcı "Tamam" butonuna basana kadar beklenip ardından uygulama sonlandırılmalıdır.

