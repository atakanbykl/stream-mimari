# Stream Mimari Proje Hafızası (Project Memory & Architecture Log)

Bu dosya, projede bugüne kadar alınan mimari kararları, uygulanan yapıları, geliştirilen modülleri ve gelecekteki yol haritasını unutmamak ve sürekliliği sağlamak için tutulan hafıza kütüğüdür.

---

## 1. Proje Kimliği ve Amacı
- **Proje Adı:** `stream-mimari`
- **Teknoloji Yığını:** Java 24, Maven (Çoklu Modül), NATS Core, NATS JetStream, Gson, JUnit 5.
- **Amaç:** Dağıtık, reaktif ve mesajlaşma tabanlı bir veri akış (stream) mimarisi oluşturmak. Düğümler (`node-*`) birbirleriyle NATS Core ve JetStream üzerinden olay (event) ve veri akışları paylaşarak haberleşir.

---

## 2. Mimari Yapı ve Modül Hiyerarşisi

```text
stream-mimari/                               <-- Kök Dizin & Parent POM (packaging: pom)
├── pom.xml                                  <-- Bağımlılık versiyon yönetimi (jnats, gson, junit)
├── GEMINI.md                                <-- Proje kuralları (BaseNode, topic formatları, özelleştirme kuralları)
├── MEMORY.md                                <-- Bu hafıza dosyası
├── .agents/
│   └── skills/
│       └── stream-node-creator/
│           └── SKILL.md                     <-- Yeni node-* oluşturma becerisi
├── base/                                    <-- Tüm node-*'ların extend ettiği çekirdek modül (jar)
│   ├── pom.xml
│   ├── README.md
│   └── src/main/java/com/stream/base/
│       ├── BaseNode.java                    <-- Yaşam döngüsü, Core/JetStream pub-sub, JSON, loglama
│       └── StreamItemHandler.java           <-- JetStream tiplendirilmiş mesaj dinleyici arayüzü
└── node-1/                                  <-- base modülünü extend eden ilk çalışan servis
    ├── pom.xml
    ├── README.md
    └── src/main/java/com/stream/node1/
        └── App.java                         <-- Örnek Core ve JetStream çoklu topic handler & publisher
```

---

## 3. Gerçekleştirilen İşlemler ve Kararlar Günlüğü

### [Adım 1] `node-1` Servisinin Oluşturulması
- Standart Maven çoklu modül dizin yapısına uygun ilk servis olarak `node-1` üretildi.
- Java 24 derleyici ayarları ve JUnit 5 test altyapısı hazırlandı.

### [Adım 2] Çekirdek `base` Modülünün Kurulması
- `node-*` servislerinin kod tekrarını önlemek amacıyla `base` isimli Maven kütüphane modülü geliştirildi.
- Kök `pom.xml` (`stream-mimari`) parent aggregator olarak yapılandırıldı.
- `BaseNode` soyut sınıfı tasarlandı:
  - `start()` ve `stop()` thread-safe yaşam döngüsü şablonu.
  - `onStart()` ve `onStop()` soyut yaşam döngüsü kancaları.
  - `[nodeId] mesaj` formatında loglama altyapısı.

### [Adım 3] NATS Core ve JetStream Pub/Sub Altyapısı
- `BaseNode` sınıfı, hem NATS Core hem JetStream için zenginleştirildi:
  - **Core NATS Dinleme:** `subscribeCore`, `subscribeCoreText`, `subscribeCoreJson`, `subscribeCore(queueGroup, ...)`
  - **Core NATS Yayınlama:** `publishCore(bytes)`, `publishCore(string)`, `publishCoreJson(object)`
  - **JetStream Stream Tanımı:** `ensureStream(name, subjects...)` (stream yoksa dinamik oluşturur)
  - **JetStream Dinleme:** `subscribeStream(handler, autoAck)`, `subscribeStream(pushOptions, ...)`, `subscribeStreamJson`
  - **JetStream Yayınlama:** `publishStream`, `publishStreamJson` (senkron PublishAck) ve `publishStreamAsync` (asenkron CompletableFuture)
  - **Kaynak Temizliği:** `stop()` tetiklendiğinde tüm abonelikler iptal edilir, dispatcher'lar ve NATS bağlantısı temiz bir şekilde kapatılır.

### [Adım 4] Antigravity Özelleştirmeleri (Self-Maintaining)
- [**`GEMINI.md`**](./GEMINI.md): Proje içi zorunlu kurallar dosyası. Tüm nodeların `BaseNode` extend etmesi, topic isimlendirmesi (`core.*`, `stream.*`) ve ihtiyaç halinde özelleştirmelerin güncellenmesi kuralı bağlandı.
- [**`stream-node-creator` (Skill)**](./.agents/skills/stream-node-creator/SKILL.md): Kullanıcı *"node-2 oluştur"* dediğinde sıfırdan tüm modül iskeletini ve `BaseNode` entegrasyonunu otomatikleştiren beceri hazırlandı.

### [Adım 5] Çift Katmanlı Mimari (NATS Core Hızı + JetStream Geçmiş Hafızası)
- **Hedef:** NATS Core ile minimum gecikmeli mesajlaşma sağlarken, JetStream'i uygulamanın geçmiş hafızası (State Store) olarak kullanmak ve herhangi bir anda bir konunun son mesajını doğrudan çekebilmek (Last-by-Subject Direct Get).
- **Risk Analizi:**
  - `RetentionPolicy.WorkQueue` geçmişi sildiği için `RetentionPolicy.Limits` zorunlu kılındı.
  - Tüketici (consumer) maliyetine girmeden son mesajı çekebilmek için `allowDirect(true)` zorunlu kılındı.
  - Canlı dinleyicilerin eski mesajlarla boğulmaması için `subscribeStreamLive` (`DeliverPolicy.New`) geliştirildi.
  - Boş konularda çökmemesi için `getLastMessage` metotları `Optional` dönecek şekilde tasarlandı.
- **Eklenen Bileşenler:**
  - `StreamRecord.java`: Payload, sequence, zaman damgası ve başlıkları taşıyan DTO.
  - `BaseNode`: `getLastMessage`, `getLastMessageText`, `getLastMessageJson`, `getLastMessageRecord`, `subscribeStreamLive`, `subscribeStreamLiveJson`, `ensureDefaultStream`.
  - `node-1`: Hem anlık komutları hem de kalıcı durum geçmişini simüle eden yeni `App.java`.

### [Adım 6] Mimari Uygunluk Denetimi (Audit) ve Tam Senkronizasyon
- **Kapsam:** Tüm projenin `stream_architecture_plan.md` standartlarına uygunluğu denetlendi ve tespit edilen boşluklar kapatıldı.
- **Yapılan İyileştirmeler:**
  - `BaseNode`: `validateCoreSubject` ve `validateStreamSubject` koruma kalkanları eklenerek `core.` ve `stream.` topic karışması engellendi. `strictTopicValidation` desteği getirildi.
  - `BaseNode`: `resolveStreamName` içinde `stream.` önekli konular doğrudan merkezi `STREAM_MIMARI` akışına yönlendirildi; mevcut stream'lerde `allowDirect` eksikse uyarı logu eklendi.
  - `.agents/skills/stream-node-creator/SKILL.md`: Yeni node üretim şablonu güncellenerek doğrudan en son çift katmanlı mimari (`ensureDefaultStream`, `subscribeStreamLiveJson`, `getLastMessageRecord`) kodlarını üretecek şekilde senkronize edildi.
  - `base/README.md` ve `node-1/README.md`: En güncel API ve servis rollerini yansıtacak şekilde revize edildi.
  - `BaseNodeTest.java`: Topic kalkanı ve stream çözümleme birim testleri eklendi.

### [Adım 7] node-1 Otomobil (Connected Car ECU) Senaryosu Uygulaması
- **Kapsam:** `node-1` servisi, akıllı bir otomobilin araç kontrol merkezini (ECU Gateway) simüle edecek şekilde kurgulandı:
  - **3 Periyodik Yayın (Publish):**
    1. `core.car.telemetry` (NATS Core): Canlı hız, RPM, gaz pedalı, vites.
    2. `stream.car.status` (JetStream): Kalıcı yakıt, batarya, kilometre, motor sıcaklığı.
    3. `core.car.alerts` (NATS Core): Acil durum, çarpışma önleme veya kritik ikazlar.
  - **4 Dinlenen Konu (Subscribe):**
    1. `core.car.commands` (NATS Core): Sürüş komutları (ACCELERATE, BRAKE).
    2. `core.car.sensors.radar` (NATS Core): Ön radar sensörü mesafe verisi.
    3. `stream.weather.current` (JetStream Canlı): Yol ve hava durumu (yağmur, buzlanma).
    4. `stream.driver.profile` (JetStream Canlı): Sürüş modu (ECO, COMFORT, SPORT).
  - **1 Geçmişten Çekilen Konu (Direct Get):**
    - `stream.car.maintenance` (JetStream): Araç marş aldığında geçmişteki son yetkili servis ve balata/yağ sağlık kaydını (`MaintenanceRecord`) doğrudan çeker. Balata < %25 veya yağ < %20 ise servis ikazı oluşturur.
  - **İş Mantığı:**
    - Radar mesafesi < 20m ve hız > 30 km/h ise acil otomatik fren devreye girer ve ikaz yayınlar.
    - Yol kaygansa ESP çekiş kontrolü aktif edilir.
    - Sürüş moduna göre gaz tepkisi ölçeklenir.

### [Adım 8] Ortak Obje / Model Kütüphanesinin `base` Modülüne Taşınması
- **Hedef:** Veri modellerinin tek bir servise (`node-1`) hapsolmasını engellemek; tüm `node-*` projelerinin erişebileceği merkezi bir obje kütüphanesi kurmak.
- **Taşınan Modeller (`com.stream.base.model.*`):**
  - `TelemetryData`, `VehicleStatus`, `AlertData`, `DriverCommand`, `RadarSensorData`, `WeatherCondition`, `DriverProfile`, `MaintenanceRecord`.
- **Entegrasyon:**
  - Modeller `base/src/main/java/com/stream/base/model/` altına taşındı.
  - `node-1`'deki eski paket temizlendi ve `import com.stream.base.model.*;` ile bağlandı.
  - `GEMINI.md`, `SKILL.md` ve `base/README.md` kuralları güncellendi.
  - Yeni oluşturulacak tüm düğümler (`node-2`, `node-3`, vb.) `base` bağımlılığı sayesinde bu modellere doğrudan erişebilir hale getirildi.

---

## 4. Kodlama ve Mesajlaşma Standartları
1. **Topic İsimlendirme:**
   - Core NATS: `core.<kategori>.<olay>` (Örn: `core.telemetry`, `core.commands`)
   - JetStream: `stream.<kategori>.<olay>` (Örn: `stream.orders.new`, `stream.alerts`)
2. **Onaylama (ACK):**
   - JetStream aboneliklerinde `autoAck=false` açıldıysa iş mantığı sonunda `msg.ack()` çağrılmalı; hata durumunda `msg.nak()` ile mesaj kuyrukta tutulmalıdır.
3. **JSON Serileştirme:**
   - Dış bağımlılık eklemek yerine `BaseNode` içindeki `toJson(...)` ve `fromJson(...)` metotları kullanılmalıdır.
4. **Node Kapatma:**
   - Uygulama ana metotlarında `Runtime.getRuntime().addShutdownHook(new Thread(node::stop));` kullanılmalıdır.

---

## 5. Yol Haritası ve Gelecek İhtiyaçlar (Backlog)
- [ ] Yeni düğümlerin (`node-2`, `node-3`, vb.) eklenmesi ve düğümler arası veri akışının test edilmesi.
- [ ] NATS sunucusu ayarları ve ortam değişkenlerinden (Environment Variables / Config) NATS URL okuma desteği.
- [ ] Mesaj yeniden deneme (retry) ve Dead Letter Queue (DLQ) mekanizması.
- [ ] İhtiyaç duyuldukça yeni Skill, Rule, Hook ve MCP entegrasyonlarının hafızaya ve projeye işlenmesi.
