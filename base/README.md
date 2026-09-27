# Base Modülü (NATS Core & JetStream Çift Katmanlı Altyapı)

Tüm `node-*` projelerinin extend edeceği çekirdek kütüphanedir.

## Mimari Prensipler
1. **NATS Core (Anlık / Minimum Gecikme):** Bellek içi (in-memory) pub/sub. Telemetri ve anlık komutlar için sub-millisecond gecikme. `core.>` topic öneki kullanılır.
2. **JetStream (Geçmiş Hafızası & State Store):** Kalıcı mesaj depolama. Canlı dinleme ve anında geçmişten son durumu çekme (Direct Get / Last-by-Subject). `stream.>` topic öneki kullanılır.

---

## Sağlanan Yetenekler ve API Referansı

### 1. NATS Core (Minimum Gecikme)
- **Çoklu Topic Dinleme:**
  - `subscribeCore(subject, handler)`: Belirtilen konuya özel `MessageHandler` atar.
  - `subscribeCore(subject, queueGroup, handler)`: Yük dengeleme (Queue Group) ile dinleme.
  - `subscribeCoreText(subject, consumer)`: Düz metin işleyen pratik dinleyici.
  - `subscribeCoreJson(subject, Class<T>, consumer)`: Otomatik JSON dönüşümlü dinleyici.
- **Çoklu Topic Yayınlama:**
  - `publishCore(subject, byte[])`
  - `publishCore(subject, String)`
  - `publishCoreJson(subject, Object)` (Dahili Gson)
  - `publishCore(subject, replyTo, byte[])` (İstek/Yanıt deseni)
- **Abonelik İptali:** `unsubscribeCore(subject)`

### 2. JetStream (Geçmiş Hafızası & State Store)
- **Stream Hazırlığı:**
  - `ensureDefaultStream()`: `stream.>` desenini kapsayan merkezi `STREAM_MIMARI` akışını oluşturur veya doğrular (`allowDirect=true` ve `RetentionPolicy.Limits` ile).
  - `ensureStream(streamName, subjects...)`: Özel isimli stream tanımlar.
- **Geçmişten Mesaj Sorgulama (Direct Get / Non-Destructive):**
  - `getLastMessageRecord(subject, Class<T>)`: Konunun geçmişteki son mesajını `Optional<StreamRecord<T>>` olarak döner.
  - `getMessageRecordBySequence(sequence, Class<T>)`: Belirtilen sıra numarasındaki (sequence) geçmiş mesajı döner.
  - `getFirstMessageRecord(subject, Class<T>)`: Konunun ilk mesajını döner.
  - `getNextMessageRecord(sequence, subject, Class<T>)`: Belirtilen sıra numarasından sonraki mesajı döner.
  - `getHistoricalRecords(subject, Class<T>, limit)`: Konunun tüm geçmişini veya son N kaydını kronolojik liste (`List<StreamRecord<T>>`) olarak çeker.
  - *Tahribatsızlık İlkesi:* Direct Get stream hafızasını asla bozmaz, mesajları silmez; çekilen veriler stream'de kalmaya devam eder ve tekrar tekrar çekilebilir.
- **Canlı Dinleme ve Yeniden Oynatma (Live & Replay):**
  - `subscribeStreamLiveJson(subject, Class<T>, handler, autoAck)`: Yalnızca yeni gelen mesajları dinler (`DeliverPolicy.New`).
  - `subscribeStreamReplayJson(subject, Class<T>, handler, autoAck)`: Konunun tüm geçmişini en baştan itibaren oynatır (`DeliverPolicy.All`).
  - `subscribeStreamFromSequenceJson(subject, startSeq, Class<T>, handler, autoAck)`: Belirtilen sıra numarasından itibaren dinler (`DeliverPolicy.ByStartSequence`).
  - *Tüketici ACK Güvencesi:* Tüketicilerin ACK vermesi mesajları stream'den KESİNLİKLE SİLMEZ (`RetentionPolicy.Limits`).
- **Yayınlama (Publish):**
  - `publishStream(subject, byte[])`, `publishStream(subject, text)`, `publishStreamJson(subject, obj)` (Senkron `PublishAck`)
  - `publishStreamAsync(...)`, `publishStreamJsonAsync(...)` (Asenkron `CompletableFuture<PublishAck>`)

### 3. Topic Ad Alanı Kalkanı (Validation)
- `validateCoreSubject(subject)`: Konunun `core.` standartlarına uyduğunu, yanlışlıkla `stream.` içermediğini kontrol eder.
- `validateStreamSubject(subject)`: Konunun `stream.` standartlarına uyduğunu kontrol eder.
- `setStrictTopicValidation(boolean)`: Kural ihlallerinde uyarı yerine `IllegalArgumentException` fırlatılmasını sağlar.

### 4. Yaşam Döngüsü ve Yardımcılar
- `start()` / `stop()`: Thread-safe durum takibi ve bağlantı/abonelik temizliği.
- `toJson()`, `fromJson()`: Dahili Gson desteği.
- `log(message)`: `[nodeId] mesaj` formatında loglama.

### 5. Ortak Model Kütüphanesi (`com.stream.base.model.*`)
Tüm `node-*` projeleri `base` modülünü bağımlılık olarak aldığı için ortak veri sözleşmelerine doğrudan erişebilir:
- `TelemetryData`: Canlı telemetri (hız, devir, gaz, vites).
- `VehicleStatus`: Kalıcı durum (yakıt, batarya, kilometre, sıcaklık).
- `AlertData`: Güvenlik ve acil fren ikazları.
- `DriverCommand`: Şoför sürüş komutları (ACCELERATE, BRAKE).
- `RadarSensorData`: Ön radar sensör mesafesi ve bağıl hız.
- `WeatherCondition`: Canlı hava ve yol durumu.
- `DriverProfile`: Sürüş modu profili (ECO, COMFORT, SPORT).
- `MaintenanceRecord`: Geçmiş servis ve balata/yağ sağlık kaydı.
