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

### [Adım 9] Maven Çoklu Modül Derleme Çözümü ve Offline Test Güvenliği
- **Karşılaşılan Durum:** NetBeans üzerinden `node-1` tek başına derlendiğinde `Could not find artifact com.stream:base:jar:1.0-SNAPSHOT` hatası alındı.
- **Neden:** `node-1`, `base` modülüne bağımlıdır. Çoklu modül projelerinde alt modüller henüz yerel Maven önbelleğine (`~/.m2/repository`) kurulmamışken tekil derlenirse Maven bağımlılığı bulamaz.
- **Çözüm:** 
  - NetBeans Maven CLI (`mvn clean install`) kök `stream-mimari` projesi üzerinde çalıştırılarak reaktör sırasına göre (`stream-mimari` -> `base` -> `node-1`) tüm kütüphaneler yerel `.m2` deposuna derlendi ve kuruldu (`BUILD SUCCESS`).
### [Adım 11] NATS & JetStream Sunucu Yapılandırması (nats-server.conf)
- **Hedef:** Çift katmanlı mimarinin (NATS Core + JetStream kalıcı hafıza ve Direct Get) ihtiyaç duyduğu sunucu altyapısını optimize bir konfigürasyonla hazır hale getirmek.
- **Eklenen Dosyalar:**
  - [`nats-server.conf`](./nats-server.conf): 
    - `port: 4222` istemci portu.
    - `http_port: 8222` HTTP izleme portu (`/varz`, `/jsz`, `/connz`).
    - `jetstream { store_dir: "./nats-data", max_mem: 1G, max_file: 10G }` kalıcı durum hafızası.
    - `max_payload: 8MB` telemetri ve JSON paket desteği.
  - [`start-nats.bat`](./start-nats.bat) ve [`start-nats.ps1`](./start-nats.ps1): Tek tıkla NATS sunucusunu başlatan Windows betikleri.
  - [`.gitignore`](./.gitignore): NATS çalışma zamanı verisi (`nats-data/`) ve loglar göz ardı edildi.

### [Adım 12] node-1 Canlı Kontrol ve Mesajlaşma Gösterge Paneli (GUI)
- **Hedef:** `node-1`'in anlık olarak yaptığı işlemleri (aldığı mesajlar, attığı mesajlar, geçmişten çektiği mesajlar) tek bir görsel pencerede canlı göstermek.
- **Eklenen Bileşenler:**
  - [`NodeActivityListener.java`](./node-1/src/main/java/com/stream/node1/gui/NodeActivityListener.java): `onMessageReceived`, `onMessageSent`, `onHistoricalRetrieved` ve `onStateChanged` arayüzü.
  - [`Node1Dashboard.java`](./node-1/src/main/java/com/stream/node1/gui/Node1Dashboard.java): Swing tabanlı modern kontrol paneli:
    - **Üst Panel:** Hız, devir, vites, yakıt, batarya, mod ve NATS canlı bağlantı durumu kartları.
    - **Sekme 1 (📥 Alınan Mesajlar):** Dinlenen 4 topic'in canlı gelen verileri (Zaman, Katman, Topic, Payload).
    - **Sekme 2 (📤 Atılan Mesajlar):** Yayınlanan 3 topic'in periyodik giden verileri (Zaman, Katman, Topic, Payload).
    - **Sekme 3 (📜 Geçmişten Çekilen):** `stream.car.maintenance` konusunun JetStream geçmiş hafızasından doğrudan çekilen sıra no, zaman damgası ve kayıt detayları (Direct Get).
    - **Sekme 4 (📋 Tüm Olay Günlüğü):** Konsol tarzı anlık olay akışı.
    - **Alt Panel:** Hızlı simülasyon butonları (Gaz ver, Fren yap, Radar testi, Geçmişi çek, Manuel yayınla, Temizle).
  - [`App.java`](./node-1/src/main/java/com/stream/node1/App.java): Başlatılırken masaüstü ortamında (`!GraphicsEnvironment.isHeadless()`) GUI'yi otomatik açacak şekilde bağlandı.

### [Adım 13] Merkezi ve Hiyerarşik Konfigürasyon Yönetimi (NodeConfig)
- **Hedef:** Tüm NATS bağlantı ayarları (url, reconnection), Stream tanımları, yayınlanan (publish) ve dinlenen (subscribe) topic isimleri, periyotlar ve eşik değerlerinin koda hardcoded gömülmesini önlemek; harici/dahili dosyalardan dinamik okunmasını sağlamak.
- **Eklenen Bileşenler:**
  - [`NodeConfig.java`](./base/src/main/java/com/stream/base/config/NodeConfig.java): Hiyerarşik konfigürasyon yöneticisi:
    - Öncelik Sırası: `System Properties (-D)` > `Ortam Değişkenleri (ENV)` > `Dış Dosya (application.properties)` > `Classpath İçi (src/main/resources)` > `Varsayılanlar (Default fallback)`.
    - Tip güvenli metotlar: `getString`, `getInt`, `getLong`, `getDouble`, `getBoolean`, `getStringList`.
  - [`BaseNode.java`](./base/src/main/java/com/stream/base/BaseNode.java): `NodeConfig config` entegrasyonu sağlandı. `buildNatsOptions()` ve `ensureDefaultStream()` ayarları konfigürasyondan dinamik çekilecek şekilde güncellendi.
  - [`application.properties`](./node-1/src/main/resources/application.properties): `node-1` için NATS URL'i, Stream adı, 3 yayın topic'i, 4 dinleme topic'i, 1 geçmiş topic'i, yayın periyodu (1000ms) ve araç güvenlik eşikleri dosyaya aktarıldı.
  - [`application.properties`](./node-1/application.properties): Projeyi yeniden derlemeye gerek kalmadan anlık topic ve sunucu değiştirebilmek için çalışma dizini kopyası hazırlandı.
  - [`App.java`](./node-1/src/main/java/com/stream/node1/App.java) ve [`Node1Dashboard.java`](./node-1/src/main/java/com/stream/node1/gui/Node1Dashboard.java): Tüm hardcoded string'ler kaldırılarak `config` değişkenlerine bağlandı.
  - [`GEMINI.md`](./GEMINI.md) & [`SKILL.md`](./.agents/skills/stream-node-creator/SKILL.md): Kural 6 ve yeni düğüm üretim şablonu güncellendi.

### [Adım 14] Tekil Dış Dosya (External File) ve Modal Hata Diyaloglu Doğrulama Mekanizması
- **Hedef:** Konfigürasyon dosyasını tamamen derleme gerektirmeyen tek bir dış dosya (`node-1/application.properties`) haline getirmek; gömülü classpath kaynaklarını silmek. Çalışma zamanında config dosyası yoksa, okunamazsa veya parametreler eksik/hatalıysa kullanıcıyı modal pop-up pencere ile uyarıp "Tamam" tıklandıktan sonra uygulamayı sonlandırmak.
- **Yapılan İşlemler:**
  - **Dahili Kaynak Temizliği:** `node-1/src/main/resources/application.properties` tamamen silindi.
  - **Harici Dosya Güvencesi:** Yalnızca çalışma dizininde bulunan `node-1/application.properties` geçerli kılındı. Parametre değişiklikleri Maven derlemesi gerektirmeksizin bir sonraki çalıştırmada doğrudan devreye girer.
  - **Katı Doğrulama ve Hata Yakalama:**
    - [`ConfigValidationException.java`](./base/src/main/java/com/stream/base/config/ConfigValidationException.java) geliştirildi.
    - [`ConfigErrorHandler.java`](./base/src/main/java/com/stream/base/config/ConfigErrorHandler.java) geliştirildi: Masaüstü ortamında `JOptionPane.showMessageDialog` ile bloklayıcı modal hata uyarısı gösterir, kullanıcı "Tamam" diyene kadar bekler ve ardından `System.exit(1)` çağırır. Test ortamları için `exitOnFatalError` ve `suppressDialogForTesting` bayrakları sağlandı.
    - [`NodeConfig.java`](./base/src/main/java/com/stream/base/config/NodeConfig.java): Sadece dış dosya sistemini tarayacak şekilde güncellendi. `requireExternalFile(title)`, `getRequiredTopic(key, prefix)`, `getRequiredInt(key, min, max)`, `getRequiredDouble(key, min, max)`, `getRequiredBoolean(key)` ve `validateAndEnforce(title)` metotları eklendi.
    - [`App.java`](./node-1/src/main/java/com/stream/node1/App.java): Başlatılırken dış dosyanın varlığını ve tüm 8 topic'in (`core.` / `stream.` mimari standartları) ve eşik değerlerinin doğrulanmasını zorunlu kıldı.
  - **Birim Testleri:** `NodeConfigTest` ve `AppTest` güncellendi, 20 birim testinin tamamı başarıyla (`BUILD SUCCESS`) geçti.

---

## 4. Kodlama ve Mesajlaşma Standartları
1. **Topic İsimlendirme:**
   - Core NATS: `core.<kategori>.<olay>` (Örn: `core.telemetry`, `core.commands`)
   - JetStream: `stream.<kategori>.<olay>` (Örn: `stream.orders.new`, `stream.alerts`)
2. **Onaylama (ACK):**
   - JetStream aboneliklerinde `autoAck=false` açıldıysa iş mantığı sonunda `msg.ack()` çağrılmalı; hata durumunda `msg.nak()` ile mesaj kuyrukta tutulmalıdır.
3. **JSON Serileştirme:**
   - Dış bağımlılık eklemek yerine `BaseNode` içindeki `toJson(...)` ve `fromJson(...)` metotları kullanılmalıdır.
4. **Konfigürasyon Yönetimi:**
   - Topic adları, NATS URL'leri ve periyotlar asla Java koduna hardcoded yazılmamalı; `BaseNode.getConfig()` (`NodeConfig`) ve dış `application.properties` üzerinden okunmalıdır.
5. **Node Kapatma:**
   - Uygulama ana metotlarında `Runtime.getRuntime().addShutdownHook(new Thread(node::stop));` kullanılmalıdır.

---

## 5. Yol Haritası ve Gelecek İhtiyaçlar (Backlog)
- [ ] Yeni düğümlerin (`node-2`, `node-3`, vb.) eklenmesi ve düğümler arası veri akışının test edilmesi.
- [x] NATS sunucusu ayarları ve ortam değişkenlerinden (Environment Variables / Config) NATS URL ve topic okuma desteği (`NodeConfig`).
- [x] Tekil dış dosya tabanlı yapılandırma ve modal hata bildirimli çalışma zamanı doğrulama altyapısı.
- [ ] Mesaj yeniden deneme (retry) ve Dead Letter Queue (DLQ) mekanizması.
- [ ] İhtiyaç duyuldukça yeni Skill, Rule, Hook ve MCP entegrasyonlarının hafızaya ve projeye işlenmesi.


