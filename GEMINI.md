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

## 3. NATS Core ve JetStream Kullanım Standartları
- **Topic İsimlendirme Standartları:**
  - NATS Core konuları için: `core.<kategori>.<olay>` (Örn: `core.telemetry`, `core.commands`)
  - JetStream konuları için: `stream.<kategori>.<olay>` (Örn: `stream.orders.new`, `stream.alerts`)
- **JetStream Stream Tanımı:**
  - JetStream topic'lerine abone olmadan veya yayın yapmadan önce mutlaka `ensureStream("STREAM_ADI", "stream.pattern.*")` çağrılmalıdır.
- **Onaylama (ACK) Politikası:**
  - `autoAck = false` ile açılan JetStream aboneliklerinde mesaj işlendikten sonra mutlaka `msg.ack()` çağrılmalı; hata durumunda `msg.nak()` ile yeniden kuyruğa alınmalıdır.

## 4. Serileştirme ve JSON
- JSON dönüşümleri için `BaseNode` içerisindeki dahili `toJson(...)` ve `fromJson(...)` metotları kullanılmalıdır.
- Tip güvenli abonelikler için `subscribeCoreJson(...)` ve `subscribeStreamJson(...)` tercih edilmelidir.

## 5. Antigravity Özelleştirmeleri (Self-Maintaining Customizations)
- Projede yeni bir iş akışı, kural, araç veya entegrasyon ihtiyacı doğduğunda ilgili özelleştirme proaktif olarak güncellenmeli veya yenisi oluşturulmalıdır:
  - **Beceriler (Skills):** Tekrarlanabilir karmaşık iş akışları, iskeleleme (scaffolding) veya operasyonlar için `.agents/skills/<ad>/SKILL.md` güncellenmeli/oluşturulmalıdır.
  - **Kurallar (Rules):** Kodlama ve mimari standartları güncellendiğinde `GEMINI.md` veya `.agents/rules/` dosyaları güncellenmelidir.
  - **Kancalar (Hooks):** İşlemler öncesi/sonrası otomatik doğrulama, derleme veya biçimlendirme gerektiğinde `.agents/hooks.json` tanımlanmalıdır.
  - **MCP:** NATS CLI, Kafka veya harici servis araçları bağlandığında `.agents/mcp_config.json` güncellenmelidir.
  - **Hafıza Dosyası (Memory Log):** Projede yapılan mimari değişiklikler, eklenen özellikler ve yol haritası `MEMORY.md` dosyasında güncel tutulmalıdır.
