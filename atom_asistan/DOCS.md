# Atom Asistan Bridge

AtomS3R (M5Stack K147) sesli asistanının köprüsü. Cihazdan gelen mikrofon sesini
OpenAI Realtime API'ye iletir, dönen sesi cihaza gönderir, ve modelin fonksiyon
çağrılarını Home Assistant'a uygular.

Home Assistant'a Supervisor üzerinden erişir — uzun ömürlü erişim tokenı
oluşturmana gerek yoktur.

## Ayarlar

| Ayar | Açıklama |
|---|---|
| `openai_api_key` | https://platform.openai.com/api-keys adresinden alınan anahtar. **Zorunlu.** |
| `openai_model` | Varsayılan `gpt-realtime-2.1`. |
| `voice` | Asistanın sesi. `marin` ve `cedar` en doğal olanlar. |
| `session_idle_timeout` | Yanıt bittikten sonra oturumun açık kalacağı saniye. Bu süre içinde butona basmadan takip sorusu sorabilirsin. Süre dolunca oturum kapanır (boşa ücret yazmaz). |
| `expose_domains` | Modelin görebileceği HA alan adları. Listeyi dar tutmak hem gizlilik hem doğruluk için iyidir. |
| `max_entities` | Modele verilecek en fazla varlık sayısı. |
| `name_filter` | Boş bırakılırsa tüm domainler listelenir. Anahtar kelime yazarsan (örn `["salon","mutfak"]`) sadece eşleşenler verilir. |
| `wake_word_enabled` | Faz 3. Önce Dockerfile'daki pip satırının yorumunu kaldırıp Rebuild etmen gerekir. |
| `instructions` | Asistanın karakteri. Boş bırakılırsa yerleşik Türkçe/İngilizce çift dilli kişilik kullanılır. |

## Ağ

Add-on 8765 portunu dinler. AtomS3R firmware'indeki `BRIDGE_HOST` alanına
Home Assistant'ın IP adresini yaz, `BRIDGE_PORT` 8765 kalsın.

## Wake word (Faz 3)

1. `Dockerfile` içindeki `# RUN pip install ... openwakeword ...` satırının
   başındaki `#` işaretini kaldır.
2. Add-on sayfasında üç nokta → **Rebuild**.
3. Ayarlardan `wake_word_enabled: true`.
4. Firmware'de `#define ALWAYS_STREAM 1` yapıp tekrar yükle.

## Atom Desk sürüm bildirimi

Köprü, GitHub'daki bir sürüm işaretine bakıp son Atom Desk sürümünü
`atom/hass/desk/guncelleme` konusuna retained yazar. Windows uygulaması bu
mesajı görüp kendini günceller. Köprü güncellemeyi **kendi yapmaz**, sadece
haber verir.

Sürüm işareti bu deponun kökünde (`atom_desk_surum.py`) duruyor, uygulamanın
kendi deposunda değil: `raw.githubusercontent.com` kimlik bilgisi kabul
etmiyor, yani private bir depoya 404 dönüyor. Dışarı çıkan tek şey bir sürüm
numarası oluyor.

| Ayar | Ne işe yarar |
|---|---|
| `desk_guncelleme_enabled` | Özelliği kapatır/açar. MQTT (`notify_enabled`) kapalıysa zaten çalışmaz. |
| `desk_depo` | `kullanici/depo` biçiminde, **public** olmalı. Varsayılan `Arda-Apps/atom-asistan-addon`. |
| `desk_surum_yolu` | Sürüm işaretinin depo içindeki yolu. Varsayılan `atom_desk_surum.py`. |
| `desk_dal` | Hangi daldan okunsun. Varsayılan `main`. |
| `desk_kontrol_saat` | Kaç saatte bir bakılsın (1–168). |

Logda `Atom Desk surum dosyasi bulunamadi (404)` görüyorsan depo private
olmuştur; `raw.githubusercontent.com` kimlik bilgisi olmadan private
depoya 404 döner.

## Sorun giderme

| Log satırı | Anlamı |
|---|---|
| `HA katalogu: 0 varlik` | `expose_domains` boş ya da `homeassistant_api` kapalı. |
| `SUPERVISOR_TOKEN yok` | `config.yaml`'da `homeassistant_api: true` eksik. |
| `OpenAI hata: ...` | Anahtar geçersiz, bakiye bitmiş veya model adı yanlış. |
| `Cihaz baglandi` görünmüyor | Firmware'deki `BRIDGE_HOST` yanlış ya da cihaz WiFi'ye bağlanmamış. |
