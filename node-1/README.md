# node-1 Servisi

`base` modülündeki `BaseNode` sınıfını extend eden ve çift katmanlı mesajlaşma mimarisini uygulayan çalışan servis implementasyonu.

## Mimarideki Rolü

1. **NATS Core (Ultra Düşük Gecikme):**
   - `core.commands` konusuna abone olarak anlık komutları (`CoreCommand`) sıfır disk gecikmesiyle yakalar.
   - `publishCoreJson` ile hızlı komut sinyalleri yayınlar.

2. **JetStream (Uygulama Geçmiş Hafızası / State Store):**
   - `ensureDefaultStream()` ile `stream.>` akışının varlığını garantiye alır.
   - `subscribeStreamLiveJson("stream.device.state", DeviceState.class, ...)` ile eski geçmişle boğulmadan yalnızca canlı yeni durumları dinler.
   - `publishStreamJson` ile durum güncellemelerini (`DeviceState`) kalıcı hafızaya yazar.
   - **Doğrudan Geçmiş Sorgusu:** `getLastMessageRecord("stream.device.state", DeviceState.class)` çağırarak herhangi bir anda cihazın geçmişteki son durumunu, zaman damgasını ve sıra numarasını doğrudan çeker.

## Başlatma ve Çalıştırma
Ana sınıf: `com.stream.node1.App`
NetBeans üzerinden `node-1` projesini çalıştırabilir veya terminalden başlatabilirsiniz.
