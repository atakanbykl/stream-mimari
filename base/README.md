# Base Modülü

Stream mimarisi içerisinde yer alan tüm `node-*` (ör. `node-1`, `node-2`, vb.) servislerinin extend edeceği çekirdek kütüphanedir.

## Özellikler
- **`BaseNode`**: Tüm düğümlerin miras alacağı temel soyut sınıf. Düğüm yaşam döngüsü (`start()`, `stop()`), NATS yapılandırması ve standart loglamayı sağlar.
- Ortak bağımlılıklar (`jnats`, `gson`, vb.) bu modül üzerinden transitive olarak tüm node projelerine aktarılır.
