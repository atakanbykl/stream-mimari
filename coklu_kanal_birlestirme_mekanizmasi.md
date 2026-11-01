# Dinamik Çoklu Kanal Birleştirme Mekanizması (Multi-Channel Aggregator)

## 1. Genel Bakış ve Amaç

Dağıtık ve olay tabanlı (event-driven) sistemlerde, farklı kaynaklardan bağımsız zamanlarda akan birden fazla veri kanalının (topic/kanal/kanal grubu) tek bir birleşik veri paketi haline getirilmesi sıkça karşılaşılan bir gereksinimdir.

Klasik yaklaşımlarda her bir kanaldan yeni veri geldiğinde anında birleşik çıktı üretmek, özellikle kanallar peş peşe güncellendiğinde gereksiz işlem ve iletim yüküne yol açar; üstelik aradaki adımlarda eksik veya yarı güncellenmiş ara durumların yayılmasına sebep olur.

Bu mekanizma; $2$ ile sonsuz ($N \ge 2$) arasında dinamik sayıda bağımsız kanaldan gelen verileri toplayıp, gereksiz tekrarları önleyerek ve **hiçbir ara güncellemeyi kaybetmeden** en uygun anda tek bir birleşik çıktı halinde dışarı aktaran genel amaçlı bir durum makinesidir.

---

## 2. Karşılaşılan Temel Zorluklar

1. **İletim Çarpanı (Burst Yükü):** Örneğin 4 kanal birbirine çok yakın milisaniyelerde güncellendiğinde, saf tetikleme modeli 4 ayrı çıktı üretir. Bu durum alıcı sistemlerde gereksiz CPU ve ağ trafiği yaratır.
2. **Yarı/Eksik Ara Durumlar (Transient States):** Kanallardan biri güncellenip diğerleri henüz gelmeden çıktı verilirse, dış dünya sistemin tutarsız (bir kısmı yeni, bir kısmı eski) bir anlık görüntüsünü okur.
3. **Aynı Kanalın Üst Üste Güncellenmesi (Ezilme / Veri Kaybı Riski):** Diğer kanallardan veri beklenirken aynı kanal peş peşe yeni veriler üretirse, eski verinin üzerine yazılması durumunda önceki bilgi kaybolabilir.

---

## 3. Çalışma Mantığı ve 3 Temel Kural

Mekanizma, izlenmesi istenen $N$ adet kanalı bir bekleme penceresi içinde takip eder. Herhangi bir kanaldan veri geldiğinde sistem şu üç kuralı sırasıyla işletir:

```
                                [ Yeni Veri Geldi ]
                                         │
                                         ▼
                       ┌───────────────────────────────────┐
                       │ Bu kanal aktif pencerede zaten    │
                       │ var mı? (Çakışma Kontrolü)        │
                       └─────────────────┬─────────────────┘
                                         │
                       ┌─────────────────┴─────────────────┐
                    EVET                                  HAYIR
                       │                                     │
                       ▼                                     ▼
        ┌──────────────────────────────┐       ┌──────────────────────────────┐
        │ 1. Önceki pencereyi (eski    │       │ Yeni veriyi kaydet ve aktif  │
        │    değerle) ANINDA İLET      │       │ pencereye ekle               │
        │ 2. Pencereyi sıfırla         │       └──────────────┬───────────────┘
        │ 3. Yeni veriyi yeni          │                      │
        │    pencerenin ilki yap       │                      │
        └──────────────┬───────────────┘                      │
                       │                                      │
                       └──────────────────┬───────────────────┘
                                          ▼
                       ┌───────────────────────────────────┐
                       │ Beklenen tüm kanallar tamamlandı  │
                       │ mı? (Tam Set / Bariyer Kontrolü)  │
                       └──────────────────┬────────────────┘
                                          │
                       ┌──────────────────┴───────────────────┐
                    EVET                                  HAYIR
                       │                                     │
                       ▼                                     ▼
        ┌──────────────────────────────┐       ┌──────────────────────────────┐
        │ Zamanlayıcıyı iptal et       │       │ Zaman aşımı sayacını başlat  │
        │ Anında birleşik çıktıyı İLET │       │ Süre dolarsa mevcut verilerle│
        │ Pencereyi sıfırla            │       │ tek sefer İLET ve sıfırla    │
        └──────────────────────────────┘       └──────────────────────────────┘
```

---

### Kural 1: Çakışma Anında Önceki Veriyi İletme (Collision-Flush)
- **Amaç:** Veri kaybını kesin olarak önlemek.
- **İşleyiş:** Aktif pencere henüz diğer kanallarla tamamlanmamışken, o pencerede **daha önce güncellenmiş bir kanal tekrar güncellenirse** (örneğin A kanalı gelmişti, diğerleri beklenirken tekrar A kanalı geldi):
  1. Yeni gelen değer eski değerin üzerine yazılmadan **önce**, mevcut penceredeki önceki değerlerle birleşik çıktı derhal oluşturulup iletilir.
  2. Aktif pencere sıfırlanır.
  3. Yeni gelen değer belleğe alınır, yeni pencerenin ilk üyesi yapılır ve diğer kanallar için süreç baştan başlatılır.
- **Sonuç:** Aynı kanaldan gelen hiçbir ara güncelleme ezilmez, kronolojik sıra bozulmadan aktarılır.

---

### Kural 2: Tam Set Bariyeri (Full-Set Barrier)
- **Amaç:** İdeal durumda sıfır ek gecikmeyle (0 ms) çıktı üretmek.
- **İşleyiş:** Beklenen $N$ farklı kanalın tamamından aktif pencere içinde birer güncelleme ulaştığı anda:
  1. Çalışan zaman aşımı sayacı iptal edilir.
  2. Herhangi bir bekleme süresi uygulanmadan **anında** tam birleşik çıktı iletilir.
  3. Pencere sıfırlanarak bir sonraki döngüye hazır hale getirilir.
- **Sonuç:** Bütün kanallar peş peşe geldiğinde tek bir tutarlı çıktı üretilir; fazladan bekleme süresi oluşmaz.

---

### Kural 3: Zaman Aşımı (Timeout Fallback)
- **Amaç:** Bazı kanallardan uzun süre veri gelmediğinde sistemin kilitlenmesini engellemek.
- **İşleyiş:** Kanalların sadece bir kısmı güncellendiğinde ve belirlenen süre boyunca diğer kanallardan ses çıkmadığında:
  1. Zaman aşımı tetiklenir.
  2. Mevcut en güncel veriler birleştirilerek **tek bir sefer** iletilir.
  3. Pencere temizlenir.
- **Sonuç:** Sistem yalnızca tam set geldiğinde değil, tekil veya seyrek güncellemelerde de zamanında tepki verir.

---

## 4. Senaryo Örnekleri

### Senaryo A: 4 Kanalın Peş Peşe Gelmesi (Hızlı Yol)
1. `Kanal-1` güncellenir $\rightarrow$ Pencere: `[1]`, Zamanlayıcı başlar.
2. `Kanal-2` güncellenir $\rightarrow$ Pencere: `[1, 2]`.
3. `Kanal-3` güncellenir $\rightarrow$ Pencere: `[1, 2, 3]`.
4. `Kanal-4` güncellenir $\rightarrow$ Pencere: `[1, 2, 3, 4]` (Tam Set!).
- **Sonuç:** Zamanlayıcı durdurulur, **1 adet tam ve tutarlı birleşik çıktı** anında iletilir. 4 ayrı çıktı yerine 1 çıktı üretilmiş olur.

### Senaryo B: Aynı Kanalın Üst Üste Gelmesi (Çakışma Koruması)
1. `Kanal-1 (Sürüm A)` gelir $\rightarrow$ Pencere: `[Kanal-1 (A)]`.
2. `Kanal-2 (Sürüm A)` gelir $\rightarrow$ Pencere: `[Kanal-1 (A), Kanal-2 (A)]`.
3. Diğer kanallar gelmeden `Kanal-1 (Sürüm B)` gelir.
- **Sonuç:**
  - `Kanal-1 (Sürüm B)` yazılmadan önce, mevcut durum olan `[Kanal-1 (A), Kanal-2 (A), ...]` **1. Çıktı** olarak hemen iletilir.
  - Ardından `Kanal-1 (Sürüm B)` yeni pencereye alınır.
  - Daha sonra diğer kanallar tamamlandığında `Kanal-1 (Sürüm B)` ile **2. Çıktı** iletilir.
  - Hiçbir veri kaybolmaz.

### Senaryo C: Yalnızca Tek Bir Kanalın Güncellenmesi
1. Sadece `Kanal-3` güncellenir $\rightarrow$ Pencere: `[Kanal-3]`, Zamanlayıcı başlar.
2. Belirlenen pencere süresi boyunca başka hiçbir kanaldan güncelleme gelmez.
- **Sonuç:** Süre dolduğunda `Kanal-3`'ün yeni hali ve diğer kanalların son bilinen halleriyle **1 adet birleşik çıktı** üretilir.

---

## 5. Sağlanan Temel Kazanımlar

- **Dinamik Ölçek:** $2$ kanaldan başlayarak yüzlerce kanala kadar aynı kurallarla çalışır.
- **Tam Veri Bütünlüğü:** Çakışma anında önceki durumu derhal yayma kuralı sayesinde hiçbir ara veri ezilmez veya kaybolmaz.
- **Minimum Gecikme:** Tüm kanallar geldiğinde zamanlayıcı beklemeden 0 ms gecikmeyle sonuç üretilir.
- **Sıfır Statik Bağımlılık:** Bağımsız kanallar, servisler veya nesneler için istenen sayıda ve farklı konfigürasyonlarla izole olarak çalıştırılabilir.
