# Base Modülü (NATS Core & JetStream Altyapısı)

Tüm `node-*` projelerinin extend edeceği çekirdek kütüphanedir.

## Sağlanan Yetenekler

### 1. NATS Core
- **Çoklu Topic Dinleme**: `subscribeCore(subject, handler)`, `subscribeCore(subject, queueGroup, handler)`
- **Düz Metin & JSON Dinleme**: `subscribeCoreText(subject, consumer)`, `subscribeCoreJson(subject, Class, consumer)`
- **Çoklu Topic Yayınlama**: `publishCore(subject, data)`, `publishCore(subject, text)`, `publishCoreJson(subject, obj)`, `publishCore(subject, replyTo, data)`
- **Abonelik İptali**: `unsubscribeCore(subject)`

### 2. JetStream
- **Stream Hazırlığı**: `ensureStream(streamName, subjects...)` - Stream yoksa otomatik oluşturur.
- **Çoklu Topic Dinleme**: `subscribeStream(subject, handler, autoAck)`, `subscribeStream(subject, pushOptions, handler, autoAck)`
- **Tiplendirilmiş JSON Dinleme**: `subscribeStreamJson(subject, Class, handler, autoAck)`
- **Çoklu Topic Senkron Yayın**: `publishStream(subject, data)`, `publishStream(subject, text)`, `publishStreamJson(subject, obj)`
- **Çoklu Topic Asenkron Yayın**: `publishStreamAsync(subject, data)`, `publishStreamJsonAsync(subject, obj)`
- **Abonelik İptali**: `unsubscribeStream(subject)`

### 3. Yaşam Döngüsü & Yardımcılar
- `start()`: Bağlantıyı kurar, Dispatcher'ları başlatır ve alt sınıftaki `onStart()` metodunu çağırır.
- `stop()`: Tüm Core ve JetStream aboneliklerini sonlandırır, dispatcher'ları ve NATS bağlantısını temizler.
- `toJson()`, `fromJson()`: Dahili Gson desteği.
- `log(message)`: `[nodeId] mesaj` formatında standart loglama.
