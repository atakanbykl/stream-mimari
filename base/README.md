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
- **Geçmişten Son Mesajı Çekme (Direct Get / Last-by-Subject):**
  - `getLastMessage(subject)`: Konunun geçmişteki son mesajını `Optional<MessageInfo>` olarak döner.
  - `getLastMessageText(subject)`: Son mesajı `Optional<String>` olarak döner.
  - `getLastMessageJson(subject, Class<T>)`: Son mesajı `Optional<T>` olarak döner.
  - `getLastMessageRecord(subject, Class<T>)`: Son mesajı `Optional<StreamRecord<T>>` (payload + sequence + timestamp + headers) olarak döner.
  - *Not:* Konuda henüz mesaj yoksa (cold start) çökmez, güvenle `Optional.empty()` döner.
- **Canlı Dinleme (Live Subscriber):**
  - `subscribeStreamLive(subject, handler, autoAck)`: Yalnızca bu andan itibaren gelen yeni mesajları dinler (`DeliverPolicy.New`). Eski mesajları baştan okumaz.
  - `subscribeStreamLiveJson(subject, Class<T>, handler, autoAck)`: JSON tip dönüşümlü canlı dinleme.
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
