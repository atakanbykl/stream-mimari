# node-1 Servisi

`base` modülündeki `BaseNode` sınıfını extend eden örnek servis implementasyonu.

## Özellikler
- **NATS Core Dinleme**:
  - `core.telemetry` -> Ham telemetri dinleyicisi
  - `core.commands` -> `CommandData` modeli üzerinden JSON dinleyici
- **JetStream Dinleme**:
  - `stream.orders.new` -> Manuel ACK ile sipariş dinleyicisi
  - `stream.alerts` -> Otomatik ACK ile `SensorData` alarm dinleyicisi
- **Yayınlama (Publish)**:
  - NATS Core ve JetStream üzerinden birden çok farklı topic'e eş zamanlı yayın yapma örneği.
