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
