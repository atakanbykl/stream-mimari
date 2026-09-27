# node-1 Servisi: Otomobil Araç Kontrol ve Telematik Ünitesi (ECU Gateway)

Bu servis, `base` modülündeki `BaseNode` sınıfını extend ederek akıllı bir **bağlantılı otomobilin (Connected Vehicle ECU)** kontrol merkezini simüle eder.

---

## 🚗 Mesajlaşma Mimarisi Özeti

```
                                  +-----------------------------+
                                  |     node-1 (Car ECU)        |
                                  +--------------+--------------+
                                                 |
         +---------------------------------------+---------------------------------------+
         |                                       |                                       |
    [3 PUBLISH TOPIC]                     [4 SUBSCRIBE TOPIC]                   [1 GEÇMİŞTEN ÇEKİLEN]
   (Belli Aralıklarla)                   (Her Biri Özel Handler)                 (İhtiyaç Anında - Direct Get)
         |                                       |                                       |
  * core.car.telemetry (NATS Core)        * core.car.commands (NATS Core)         * stream.car.maintenance (JetStream)
    (Hız, RPM, Gaz, Vites)                  (Şoför Gaz/Fren/Vites Komutları)        (Son servis, balata ve yağ sağlığı)
  * stream.car.status (JetStream)         * core.car.sensors.radar (NATS Core)
    (Yakıt, Batarya, KM, Sıcaklık)          (Ön radar mesafe sensörü)
  * core.car.alerts (NATS Core)           * stream.weather.current (JetStream)
    (Acil fren, çarpışma ikazı)             (Canlı hava durumu ve buzlanma)
                                          * stream.driver.profile (JetStream)
                                            (ECO, COMFORT, SPORT sürüş modu)
```

---

## ⚙️ İş Mantığı (Business Logic) Detayları

1. **Ön Çarpışma Önleme ve Acil Frenleme (Emergency Braking):**
   - `core.car.sensors.radar` üzerinden gelen engel mesafesi 20 metrenin altına düşerse ve aracın hızı > 30 km/h ise otomatik acil fren tetiklenir, hız derhal düşürülür ve `core.car.alerts` konusuna `CRITICAL` seviyeli ikaz fırlatılır.
2. **Çekiş Kontrolü (ESP):**
   - `stream.weather.current` üzerinden gelen hava durumu kaygan/buzlu ise ESP aktif edilir ve maksimum tork sınırlanır.
3. **Sürüş Modu Uyarlaması:**
   - `stream.driver.profile` ile mod `SPORT` yapıldığında gaz tepkisi %150'ye çıkar; `ECO` modunda yakıt tasarrufu için gaz yumuşatılır.
4. **Geçmiş Servis Hafızası Sorgulama (Direct Get):**
   - Araç ilk açıldığında `stream.car.maintenance` konusunun geçmişteki **en son mesajını** `getLastMessageRecord` ile doğrudan çeker.
   - Eğer geçmişteki kayıtta fren balatası sağlığı < %25 veya motor yağı ömrü < %20 ise gösterge panelinde bakım uyarısı aktif edilir.
5. **Periyodik Yayın Döngüsü:**
   - Arka planda çalışan zamanlayıcı (`ScheduledExecutorService`), her 1 saniyede bir `core.car.telemetry` ve `stream.car.status` konularını otomatik olarak yayınlar.

---

## 📂 Ortak Modeller (`com.stream.base.model.*`)

Modeller tüm nodeların erişebileceği `base` kütüphanesine taşınmıştır:
- [**`TelemetryData.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/TelemetryData.java): Canlı telemetri modeli.
- [**`VehicleStatus.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/VehicleStatus.java): Kalıcı araç durumu modeli.
- [**`AlertData.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/AlertData.java): Acil durum ve güvenlik ikazları modeli.
- [**`DriverCommand.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/DriverCommand.java): Sürüş komutları modeli.
- [**`RadarSensorData.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/RadarSensorData.java): Radar sensör modeli.
- [**`WeatherCondition.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/WeatherCondition.java): Canlı hava durumu modeli.
- [**`DriverProfile.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/DriverProfile.java): Sürüş profili modeli.
- [**`MaintenanceRecord.java`**](file:///c:/Users/ataka/OneDrive/Desktop/CodeFolders/Java/NetBeans/stream-mimari/base/src/main/java/com/stream/base/model/MaintenanceRecord.java): Geçmişten çekilen mekanik bakım kaydı modeli.
