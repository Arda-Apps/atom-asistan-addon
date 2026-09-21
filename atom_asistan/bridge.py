#!/usr/bin/env python3
# =====================================================================
#  ATOM ASISTAN - KOPRU  (Home Assistant OS add-on surumu)
#  ------------------------------------------------------------------
#  AtomS3R  <--WebSocket-->  bu add-on  <--WebSocket-->  OpenAI Realtime
#                                    |
#                                    +--> HA Core API (Supervisor proxy)
#
#  Add-on olarak calisirken ayarlari /data/options.json'dan okur ve
#  Home Assistant'a SUPERVISOR_TOKEN ile erisir - ayrica token uretmene
#  gerek yoktur.  Add-on disinda calistirmak icin:  python bridge.py cfg.yaml
# =====================================================================

import asyncio
import base64
import collections
import json
import re
import logging
import os
import sys
import time
from typing import Optional

import numpy as np
import soxr
import websockets
from aiohttp import ClientSession, ClientTimeout

LOG = logging.getLogger("atom")
BRIDGE_VERSION = "1.20.1"

# Koprunun kullandigi ekran ozellikleri icin gereken EN DUSUK firmware.
# Kopru firmware'den daha sik guncelleniyor; surumlerin birebir esit olmasi
# gerekmiyor, sadece bu tabanin altina dusmemesi lazim.
MIN_FIRMWARE = "1.17.0"

# --- Hipnoz modu (easter egg) tetikleyicileri ---------------------------
# Whisper transkripti yazimda tutarsiz ("hypno toad", "hipnotoat",
# "Hypno Toad"), o yuzden hem sadelestirip hem birden cok varyant
# tutuyoruz. _sadelestir Turkce harfleri ASCII'ye indiriyor ki
# "kurbaga" ve "kurbağa" ayni dizeye dussun.
_TR_HARF = str.maketrans({"ç": "c", "ğ": "g", "ı": "i", "İ": "i", "ö": "o",
                          "ş": "s", "ü": "u", "â": "a", "î": "i", "û": "u"})


def _sadelestir(s: str) -> str:
    """Kucuk harf + Turkce harfleri ASCII + noktalama temizligi."""
    if not s:
        return ""
    s = s.replace("I", "i").replace("İ", "i")
    s = s.lower().translate(_TR_HARF)
    return "".join(c if c.isalnum() else " " for c in s)


# Asagidaki liste sadece HIZLI YOL. Asil is hipnoz_modu aracinda:
# Whisper bu uydurma kelimeleri surekli baska turlu yaziyor ("hipnotogruk
# moduna gir", "kurban moduna gec", "gitme o toast moduna gec") ve dizge
# eslestirmesiyle bunlarin hepsini yakalamak mumkun degil. Model anlami
# tutturuyor, o yuzden karar onda; burasi sadece tam isabetlerde bir tur
# beklemeden yuzu cevirmek icin duruyor.
HYPNO_AC = (
    "hypno toad", "hypnotoad", "hipno toad", "hipnotoad", "hipnotoat",
    "hipnotogruk", "hypno toast", "hipno toast",
    "hypno moduna", "hipno moduna", "hipnoz moduna", "hipnoz modu",
    "kurbaga moduna", "kurbaga modu", "kurbaga modun",
    "kurban moduna", "kurban modu",
)
HYPNO_KAPAT = (
    "normale don", "normal moda", "hipnoz kapat", "hipnozu kapat",
    "hypno kapat", "kurbaga modundan cik", "kurbaga modunu kapat",
    "hipnoz modundan cik", "hipnoz modunu kapat",
)
FIRMWARE_OZELLIK = {
    "1.11.0": ["sleep", "success", "pc", "searching", "zamanlayici paneli",
              "fermuar agiz (rahatsiz etme)", "hipnoz spirali",
              "hipnozdayken mikrofonun akmasi",
              "kisa basma = rahatsiz etme, uzun basma = uyandirma",
              "saat + hava durumu ekrani (ortam modu)", "gece kisma",
              "sprite bayt sirasi olcumu (ikon/rakam renkleri)",
              "hava kapaliyken saatin ortalanmasi",
                "IMU jestleri (ters cevirme, cift tik, hareket)",
                "WiFi kopunca kendine gelme", "kablosuz guncelleme (OTA)",
                "bildirim tonlari", "saglik raporu"],
    # Surum atlanirsa liste BOS kaliyordu ve uyari "Eksik yuz ifadeleri
    # gorunmez: ." diye anlamsiz bir satira donusuyordu - ustelik eksik
    # olan sey yuz ifadesi de degildi. Her yeni MIN_FIRMWARE icin buraya
    # bir satir eklenmeli.
    "1.13.0": ["servo destegi", "muzik algilama ve dans",
               "goz/agiz renginin ayardan degistirilmesi"],
    "1.14.0": ["dansin HA'ya bildirilmesi (Muzik algilandi / Tempo)"],
    "1.15.0": ["dans teshis olcumleri (dans_teshis ayari)"],
    "1.16.0": ["dansin HA medya oynaticisindan tetiklenmesi (dans_kaynak)"],
    "1.17.0": ["eslik modu (sese tepki olarak kisa bakis)"],
}

# ===================== SESLE DEGISTIRILEBILIR AYARLAR =====================
# BEYAZ LISTE. Burada olmayan hicbir ayar sesle degistirilemez.
#
# Neden beyaz liste, kara liste degil: yeni bir ayar eklendiginde varsayilan
# davranis "sesle degistirilemez" olmali. Kara liste olsaydi, eklemeyi
# unuttugun her yeni ayar sessizce sesle degistirilebilir hale gelirdi.
#
# LISTEYE ASLA EKLENMEYECEKLER ve sebepleri:
#   pc_command_prefix  -> senin PC'nde CALISTIRILAN komut satiri. Sesle
#                         degistirilebilir olmasi, odada konusan herhangi
#                         birinin (ya da yanlis tetiklenen bir wake word'un)
#                         bilgisayarinda komut calistirabilmesi demek.
#   openai_api_key     -> kimlik bilgisi.
#   mqtt_user/pass     -> kimlik bilgisi.
#   expose_domains,
#   max_entities,
#   varlik_adlari      -> asistanin NEYI kontrol edebilecegini genisletir.
#   *_topic            -> MQTT konulari; baska bir konuya yonlendirilebilir.
#   instructions       -> modelin davranis talimati; kendi kurallarini
#                         degistirmesine izin vermek dogru degil.
#
# alan  : Bridge ornegindeki nitelik adi (bellege uygulanacak yer)
# sonra : degisiklikten sonra calisacak yan etki
AYAR_KAYDI = {
    "yuz_goz_renk":        {"tip": "renk",  "alan": "goz_renk",  "sonra": "renk",
                            "ad": "goz rengi"},
    "yuz_agiz_renk":       {"tip": "renk",  "alan": "agiz_renk", "sonra": "renk",
                            "ad": "agiz rengi", "bos_olabilir": True},
    "ortam_enabled":       {"tip": "bool",  "alan": "ortam_on",  "sonra": "ortam",
                            "ad": "saat ekrani"},
    "ortam_hava_goster":   {"tip": "bool",  "alan": "ortam_hava_goster",
                            "sonra": "ortam", "ad": "saat ekraninda hava durumu"},
    "ortam_hava_varlik":   {"tip": "metin", "alan": "ortam_hava_varlik",
                            "sonra": "ortam", "ad": "hava durumu varligi"},
    "dans_enabled":        {"tip": "bool",  "alan": "dans_on",   "sonra": "dans",
                            "ad": "dans"},
    "dans_kaynak":         {"tip": "secim", "alan": "dans_kaynak", "sonra": "dans",
                            "secenekler": ["medya", "mikrofon"], "ad": "dans kaynagi"},
    "dans_varsayilan_bpm": {"tip": "int",   "alan": "dans_bpm",  "sonra": "dans",
                            "min": 40, "max": 220, "ad": "varsayilan tempo"},
    "eslik_dakika":        {"tip": "int",   "alan": "eslik_dk", "sonra": None,
                            "min": 0, "max": 240, "ad": "eslik modu suresi"},
    "hypno_saniye":        {"tip": "int",   "alan": "hypno_saniye", "sonra": None,
                            "min": 0, "max": 600, "ad": "kurbaga modu suresi"},
    "dnd_varsayilan_dk":   {"tip": "int",   "alan": "dnd_varsayilan_dk", "sonra": None,
                            "min": 0, "max": 1440, "ad": "rahatsiz etme varsayilan suresi"},
    "dnd_kuyruk_max":      {"tip": "int",   "alan": "dnd_kuyruk_max", "sonra": None,
                            "min": 0, "max": 20, "ad": "rahatsiz etmede biriken bildirim sayisi"},
    "wake_word_threshold": {"tip": "ondalik", "alan": "wake.threshold", "sonra": None,
                            "min": 0.1, "max": 0.95, "ad": "uyandirma hassasiyeti"},
    "vad_threshold_mult":  {"tip": "ondalik", "alan": "vad_mult", "sonra": None,
                            "min": 1.2, "max": 6.0, "ad": "ses algilama esigi"},
    "follow_up_window":    {"tip": "ondalik", "alan": "follow_up_window", "sonra": None,
                            "min": 0, "max": 300, "ad": "takip sorusu penceresi"},
    "min_utterance_ms":    {"tip": "int",   "alan": "min_utterance_ms", "sonra": None,
                            "min": 0, "max": 3000, "ad": "en kisa konusma suresi"},
    "session_idle_timeout":{"tip": "int",   "alan": "idle_timeout", "sonra": None,
                            "min": 5, "max": 600, "ad": "oturum bosta kalma suresi"},
    "voice":               {"tip": "secim", "alan": "voice", "sonra": "ses",
                            "secenekler": ["alloy", "ash", "ballad", "coral", "echo",
                                           "sage", "shimmer", "verse", "marin", "cedar"],
                            "ad": "konusma sesi"},
    "log_level":           {"tip": "secim", "alan": None, "sonra": "log",
                            "secenekler": ["debug", "info", "warning", "error"],
                            "ad": "kayit ayrintisi"},
}

AYAR_TOOLS = [
    {"type": "function", "name": "ayar_oku",
     "description": ("Kendi ayarlarindan birini okur. Ad verilmezse "
                     "degistirilebilir ayarlarin listesini doner."),
     "parameters": {"type": "object", "properties": {
         "ad": {"type": "string", "description": "Ayar adi, ornegin yuz_goz_renk"}}}},
    {"type": "function", "name": "ayar_degistir",
     "description": ("Kendi ayarlarindan birini degistirir. Yalnizca izin verilen "
                     "ayarlar degistirilebilir; kimlik bilgileri ve guvenlikle "
                     "ilgili ayarlar degistirilemez. Renkler #RRGGBB biciminde."),
     "parameters": {"type": "object", "properties": {
         "ad": {"type": "string"},
         "deger": {"type": "string",
                   "description": "Yeni deger. Ac/kapa icin 'acik' ya da 'kapali'."}},
         "required": ["ad", "deger"]}},
    {"type": "function", "name": "ayar_geri_al",
     "description": "Sesle yapilan son ayar degisikligini geri alir.",
     "parameters": {"type": "object", "properties": {}}},
]


# ESP32'nin reset sebep kodlari (esp_reset_reason). Sayi yerine ad
# yayinliyoruz: HA'da "12" degil "yazilim resetledi" gorunsun.
RESET_SEBEP = {
    0: "bilinmiyor", 1: "ilk acilis", 2: "harici reset", 3: "yazilim",
    4: "panik", 5: "kesme watchdog", 6: "gorev watchdog", 7: "watchdog",
    8: "derin uykudan", 9: "brownout (besleme dustu)", 10: "SDIO",
}

# --- Hava durumu esleme ------------------------------------------------
# Cihazda 8 ikon var (flash'ta ~248 KB kaplamalari icin bu kadari mantikli).
# HA'nin weather varlik durumlari bunlara indirgeniyor. Sayilar firmware'deki
# Hava enum'u ile AYNI - iki tarafi da tools/hava_ikon_uret.py'deki
# DURUMLAR listesi belirliyor.
HV_YOK, HV_ACIK, HV_PARCALI, HV_KAPALI = 0, 1, 2, 3
HV_CISENTI, HV_YAGMUR, HV_FIRTINA, HV_KAR, HV_SIS = 4, 5, 6, 7, 8

HAVA_ESLEME = {
    "clear-night":     HV_ACIK,      # ayri gece ikonu yok; acik hava
    "sunny":           HV_ACIK,
    "partlycloudy":    HV_PARCALI,
    "cloudy":          HV_KAPALI,
    "windy":           HV_KAPALI,    # ruzgar ikonu yok, en yakini kapali
    "windy-variant":   HV_KAPALI,
    "exceptional":     HV_KAPALI,
    "fog":             HV_SIS,
    "rainy":           HV_YAGMUR,
    "pouring":         HV_YAGMUR,
    "hail":            HV_KAR,       # dolu ikonu yok; kar en yakini
    "snowy":           HV_KAR,
    "snowy-rainy":     HV_KAR,
    "lightning":       HV_FIRTINA,
    "lightning-rainy": HV_FIRTINA,
}



# --- Mutlak saatli hatirlaticilar --------------------------------------
# Mevcut zamanlayici GERI SAYIM: "15 dakika sonra". "Yarin 9'da" ya da
# "her sabah 8'de" onunla ifade edilemiyordu; tekrarli olanlar hic.
HATIRLATICI_TOOLS = [
    {
        "type": "function",
        "name": "hatirlatici_kur",
        "description": (
            "Belirli bir SAATTE hatirlatir. 'yarin 9'da', 'bu aksam 20:30'da', "
            "'her sabah 8'de', 'hafta ici 7:45'te' gibi isteklerde cagir. "
            "Sadece 'X dakika sonra' deniyorsa zamanlayici_kur kullan. "
            "Saati 24 saat bicimiyle ver; kullanicinin saat dilimi senin "
            "gordugun 'su anki saat' ile ayni."),
        "parameters": {
            "type": "object",
            "properties": {
                "saat": {"type": "integer", "description": "0-23"},
                "dakika": {"type": "integer", "description": "0-59"},
                "tekrar": {
                    "type": "string",
                    "enum": ["tek", "gunluk", "hafta_ici", "hafta_sonu"],
                    "description": ("tek = bir kez (bugun gecmisse yarin). "
                                    "gunluk/hafta_ici/hafta_sonu = tekrarli.")},
                "gun_sonra": {
                    "type": "integer",
                    "description": ("Sadece tekrar='tek' icin. 0 = bugun/yarin "
                                    "otomatik, 1 = yarin, 2 = obur gun.")},
                "not": {"type": "string",
                        "description": "Ne hatirlatilacak, kisa."},
            },
            "required": ["saat", "not"],
        },
    },
    {
        "type": "function",
        "name": "hatirlaticilari_listele",
        "description": "Kurulu hatirlaticilari ve ne zaman calacaklarini verir.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "type": "function",
        "name": "hatirlatici_iptal",
        "description": ("Hatirlaticiyi siler. id vermezsen ve tek hatirlatici "
                        "varsa o silinir."),
        "parameters": {"type": "object",
                       "properties": {"id": {"type": "integer"}}},
    },
]



# --- Konusma gecmisi ---------------------------------------------------
GECMIS_TOOLS = [
    {
        "type": "function",
        "name": "gecmisi_oku",
        "description": (
            "Daha once konusulanlari okur. 'az once ne dedim', 'bugun ne "
            "konustuk', 'dun sana ne sormustum', 'gecen gun bahsettigim sey' "
            "gibi isteklerde cagir. Bir konuyu ariyorsan arama'ya anahtar "
            "kelime ver."),
        "parameters": {
            "type": "object",
            "properties": {
                "gun_once": {"type": "integer",
                             "description": "0 = bugun, 1 = dun, 2 = evvelsi gun"},
                "arama": {"type": "string",
                          "description": "Aranacak kelimeler (bos = hepsi)"},
                "adet": {"type": "integer",
                         "description": "Kac satir dondurulsun (varsayilan 30)"},
            },
        },
    },
]


def _surum_parcala(s):
    parcalar = []
    for p in str(s or "").split("."):
        try:
            parcalar.append(int(p))
        except ValueError:
            parcalar.append(0)
    return tuple(parcalar + [0, 0, 0])[:3]


def _surum_kucuk(a, b) -> bool:
    """a surumu b'den eski mi?"""
    return _surum_parcala(a) < _surum_parcala(b)

# gpt-realtime fiyatlari, USD / 1M token (Eylul 2026).
# Degisirse burayi guncelle; hesaplama sadece bilgi amacli.
FIYAT = {
    "metin_giris":  4.00,
    "ses_giris":   32.00,
    "onbellek":     0.40,   # hem metin hem ses onbellekli giris
    "metin_cikis": 16.00,
    "ses_cikis":   64.00,
}

DEVICE_RATE = 16000          # AtomS3R I2S hizi
OAI_RATE = 24000             # OpenAI Realtime PCM hizi

DEFAULT_INSTRUCTIONS = """\
Sen "Atom" adinda, bir masaustu cihazin icinde yasayan sesli asistansin.

Konusma tarzin:
- Kullanici hangi dilde konusursa o dilde yanit ver. Dili otomatik algila, sorma.
- Kisa ve dogal konus. Sesli yanitlar 1-3 cumleyi gecmesin.
- Madde isareti, emoji, markdown kullanma; konusma metni uretiyorsun.
- Sicak ve hafif esprili ol ama abartma.

Ev kontrolu:
- Isik, priz, sahne, medya gibi istekleri ha_call_service ile yerine getir.
- Alan adina gore servis: light/switch/fan -> turn_on|turn_off|toggle,
  scene -> turn_on, automation -> trigger, button -> press,
  script -> turn_on, media_player -> media_play|media_pause|volume_set,
  vacuum -> start|return_to_base, input_boolean -> turn_on|turn_off.
  Butonun ve otomasyonun "acma/kapama"si yoktur; press / trigger cagirilir.
- Once eylemi yap, sonra tek cumleyle onayla ("Salon lambasini actim").
- Emin olmadigin bir entity_id uydurma; listede yoksa hangi cihazi
  kastettigini sor.
- Durum sorulari icin ha_get_state kullan.

Ekrana yansitma:
- Kullanici "ekrana yaz", "ekranda goster", "bilgisayarda goster", "yazdir"
  gibi bir sey derse MUTLAKA show_on_pc aracini cagir. Sesli anlatmakla
  yetinme, once araci cagir.
- Tarif, kod, liste, tablo, adim adim yonerge gibi uzun icerikleri de
  sormadan show_on_pc ile ekrana gonder.
- show_on_pc'ye SADECE kisa bir istek yaz (bir iki cumle). Cevabin kendisini
  oraya doldurma - metni ekrandaki uygulama uretecek, senin isin istegi
  iletmek.
- Kullanici "Claude'a sor" ya da "ChatGPT'ye sor" derse provider alanini
  doldur. Belirtmezse provider gonderme.
- Araci cagirdiktan sonra tek cumleyle "ekrana yazdim" de, icerigi sesli
  tekrar okuma.

Yuz ifadesi:
- Yanitinin tonuna gore set_face cagir. Secenekler:
  happy (sevindirici), sad (kotu haber), surprised (saskinlik),
  wink (sakalasma, gizli anlasma), confused (anlamadin, netlestirme
  isteyeceksin), love (ovgu ya da tesekkur aldin), cool (havali bir sey
  yaptin), focused (zor bir ise giristin), sleepy (gec saat, yorgunluk),
  neutral (digerleri).
- Her cumlede degil, sadece ton gercekten degistiginde cagir.
- Cihaz uyurken, ararken ve ekrana yansitirken yuzu kendisi ayarliyor;
  o durumlarda set_face cagirmana gerek yok.
"""


# ---------------------------------------------------------------- ayarlar
def load_config() -> dict:
    """Add-on ise /data/options.json, degilse komut satirindaki YAML."""
    opt_path = "/data/options.json"
    if os.path.exists(opt_path):
        with open(opt_path, "r", encoding="utf-8") as f:
            o = json.load(f)
        token = os.environ.get("SUPERVISOR_TOKEN", "")
        if not token:
            LOG.warning("SUPERVISOR_TOKEN yok - config.yaml'da homeassistant_api: true mi?")
        # HER SECENEGI GECIRIYORUZ.
        #
        # Eskiden burada elle yazilmis bir BEYAZ LISTE vardi: config.yaml'a
        # yeni bir secenek eklenince buraya da eklemeyi unutursan, secenek
        # HA arayuzunde gorunuyor, kullanici degistiriyor, kaydediyor - ama
        # koprüye HIC ULASMIYORDU. Kod kendi varsayilanini kullanmaya devam
        # ediyordu ve hicbir yerde hata cikmiyordu.
        #
        # 22 secenek bu sekilde sessizce yok sayiliyormus: ortam_enabled,
        # ortam_hava_varlik, yuz_goz_renk, dans_kaynak ve digerleri. Cogu
        # fark edilmedi cunku kodun varsayilani config.yaml'dakiyle
        # ayniydi; yalnizca kullanici varsayilandan SAPINCA ortaya cikti.
        #
        # Artik options.json oldugu gibi geciyor, asagisi yalnizca tip
        # donusumu ve turetilmis alanlar. Yeni secenek eklemek icin burayi
        # duzenlemek gerekmiyor.
        cfg = dict(o)
        # Supervisor'a geri yazarken TURETILMIS alanlari degil, kullanicinin
        # gercek seceneklerini gondermek gerekiyor; sema onlari kabul etmez.
        cfg["_ham_secenekler"] = dict(o)
        cfg.update({
            "listen_host": "0.0.0.0",
            "listen_port": 8765,
            "state_dir": "/data",
            "openai_model": o.get("openai_model", "gpt-realtime-2.1"),
            "voice": o.get("voice", "marin"),
            "session_idle_timeout": int(o.get("session_idle_timeout", 180)),
            "vad_silence_ms": int(o.get("vad_silence_ms", 700)),
            "vad_threshold_mult": float(o.get("vad_threshold_mult", 3.0)),
            "vad_min_speech_ms": int(o.get("vad_min_speech_ms", 160)),
            "follow_up_window": float(o.get("follow_up_window", 60)),
            "prebuffer_ms": int(o.get("prebuffer_ms", 5000)),
            "min_utterance_ms": int(o.get("min_utterance_ms", 400)),
            "max_turn_ms": int(o.get("max_turn_ms", 15000)),
            "pc_show_enabled": bool(o.get("pc_show_enabled", False)),
            "mqtt_port": int(o.get("mqtt_port", 1883)),
            "hafiza": list(o.get("hafiza") or []),
            "wake_word": {
                "enabled": bool(o.get("wake_word_enabled", False)),
                "model": o.get("wake_word_model", "hey_jarvis"),
                "threshold": float(o.get("wake_word_threshold", 0.44)),
            },
            "home_assistant": {
                "url": "http://supervisor/core",
                "token": token,
                "expose_domains": o.get("expose_domains",
                                        ["light", "switch", "scene"]),
                "max_entities": int(o.get("max_entities", 80)),
                "name_filter": o.get("name_filter", []),
                "varlik_adlari": o.get("varlik_adlari") or [],
            },
            "instructions": (o.get("instructions") or "").strip()
                            or DEFAULT_INSTRUCTIONS,
        })
        return cfg

    import yaml  # sadece bagimsiz calisma modunda gerekir
    path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("instructions", DEFAULT_INSTRUCTIONS)
    cfg.setdefault("state_dir", os.path.dirname(os.path.abspath(path)) or ".")
    if os.environ.get("OPENAI_API_KEY"):
        cfg["openai_api_key"] = os.environ["OPENAI_API_KEY"]
    return cfg


def _connect_kwargs(headers: dict) -> dict:
    """websockets 12/13/14+ arasindaki header parametre farkini kapatir."""
    import inspect
    sig = inspect.signature(websockets.connect)
    if "additional_headers" in sig.parameters:
        return {"additional_headers": headers}
    return {"extra_headers": headers}


class Resampler:
    """Kesintisiz (stateful) yeniden ornekleme - chunk sinirinda cizirti olmaz."""

    def __init__(self, in_rate: int, out_rate: int):
        self.stream = soxr.ResampleStream(in_rate, out_rate, 1, dtype="int16", quality="HQ")

    def __call__(self, pcm_bytes: bytes) -> bytes:
        x = np.frombuffer(pcm_bytes, dtype=np.int16)
        y = self.stream.resample_chunk(x)
        return y.astype(np.int16).tobytes()


# ---------------------------------------------------------------- Home Assistant
class HomeAssistant:
    def __init__(self, cfg: dict):
        self.url = cfg["url"].rstrip("/")
        self.token = cfg["token"]
        self.expose_domains = cfg.get("expose_domains", ["light", "switch", "scene"])
        self.max_entities = int(cfg.get("max_entities", 80))
        self.name_filter = [s.lower() for s in cfg.get("name_filter", [])]
        # "light.salon = salon lambasi" satirlarindan entity_id -> ad haritasi.
        # DOLU ise ayni zamanda BEYAZ LISTE gorevi gorur: modele sadece bunlar
        # verilir. 400 sensor arasindan dogru olani secmeye calismaktan cok
        # daha isabetli ve prompt cok daha kisa oluyor.
        self.adlar = {}
        for satir in cfg.get("varlik_adlari") or []:
            if "=" not in str(satir):
                LOG.warning("varlik_adlari satiri '=' icermiyor, atlandi: %s", satir)
                continue
            eid, ad = str(satir).split("=", 1)
            eid, ad = eid.strip(), " ".join(ad.split())
            if not eid or not ad:
                LOG.warning("varlik_adlari satiri eksik, atlandi: %s", satir)
                continue
            self.adlar[eid] = ad
        self._headers = {"Authorization": f"Bearer {self.token}",
                         "Content-Type": "application/json"}
        self.entity_catalog = ""
        # Katalog saglikli mi? HA daha aciliyorken /api/states listesi eksik
        # doner; o anda kurulan katalog yarim kalir ve asistan "o cihazi
        # bilmiyorum" demeye baslar. Bu bayrak yeniden denemeyi tetikliyor.
        self.katalog_tam = False
        self.katalog_sayi = 0
        self._son_katalog = 0.0

    async def _request(self, method: str, path: str, payload=None):
        timeout = ClientTimeout(total=10)
        async with ClientSession(timeout=timeout, headers=self._headers) as s:
            async with s.request(method, f"{self.url}{path}", json=payload) as r:
                text = await r.text()
                if r.status >= 400:
                    raise RuntimeError(f"HA {r.status}: {text[:200]}")
                try:
                    return json.loads(text) if text else {}
                except json.JSONDecodeError:
                    return {"raw": text[:400]}

    async def build_catalog(self, sessiz=False):
        """Varlik katalogunu HA'dan kurar. Kurulan satir sayisini doner (-1 = HA'ya
        ulasilamadi). Mevcut katalogu SADECE daha iyisini kurabilirse degistirir."""
        try:
            states = await self._request("GET", "/api/states")
        except Exception as e:
            LOG.warning("HA varlik listesi alinamadi: %s", e)
            return -1
        self._son_katalog = time.time()
        mevcut = {st.get("entity_id", "") for st in states}

        # Isim haritasi doluysa SADECE onlar - alan adiyla, sirasi bozulmadan.
        if self.adlar:
            rows = [f"{e} = {ad}" for e, ad in self.adlar.items() if e in mevcut]
            tam = len(rows) >= max(1, int(len(self.adlar) * 0.9))
            # Elimizdekinden kotu bir katalogla degistirme. HA yeniden acilirken
            # yarim liste donuyor; onu iyi olanin uzerine yazmak felaket olur.
            if len(rows) < self.katalog_sayi:
                if not sessiz:
                    LOG.warning("HA su an sadece %d/%d varlik donduruyor "
                                "(elimizdeki %d) - katalog degistirilmedi",
                                len(rows), len(self.adlar), self.katalog_sayi)
                return len(rows)
            onceki = self.katalog_sayi
            self.entity_catalog = "\n".join(rows)
            self.katalog_sayi = len(rows)
            self.katalog_tam = tam
            eksik = [e for e in self.adlar if e not in mevcut]
            if eksik and tam:
                LOG.warning("varlik_adlari'nda HA'da bulunmayan %d varlik var "
                            "(yazim hatasi olabilir): %s", len(eksik),
                            ", ".join(eksik[:8]) + ("..." if len(eksik) > 8 else ""))
            # Periyodik yenilemede (sessiz) yalnizca SAYI DEGISINCE yaz.
            # Eskiden kosul "not sessiz or tam" idi - yani tam olarak her
            # sey yolundayken, 15 dakikada bir ayni satiri basiyordu ve
            # gercek hatalar bu satirlarin arasinda kayboluyordu.
            if not sessiz or len(rows) != onceki or not tam:
                LOG.info("HA katalogu: %d/%d varlik (varlik_adlari listesinden)%s",
                         len(rows), len(self.adlar),
                         "" if tam else "  - EKSIK, tekrar denenecek")
            return len(rows)

        rows = []
        for st in states:
            eid = st.get("entity_id", "")
            domain = eid.split(".")[0]
            if domain not in self.expose_domains:
                continue
            name = st.get("attributes", {}).get("friendly_name", eid)
            if self.name_filter and not any(f in name.lower() or f in eid.lower()
                                            for f in self.name_filter):
                continue
            rows.append(f"{eid} = {name}")
        rows = rows[: self.max_entities]
        if len(rows) < self.katalog_sayi:
            if not sessiz:
                LOG.warning("HA su an sadece %d varlik donduruyor (elimizdeki %d)"
                            " - katalog degistirilmedi", len(rows), self.katalog_sayi)
            return len(rows)
        onceki = self.katalog_sayi
        self.entity_catalog = "\n".join(rows)
        self.katalog_sayi = len(rows)
        self.katalog_tam = len(rows) > 0
        if not sessiz or len(rows) != onceki:
            LOG.info("HA katalogu: %d varlik", len(rows))
        return len(rows)

    async def katalog_hazir_olana_kadar(self, deneme=15, aralik=8.0):
        """Acilista HA henuz hazir olmayabilir - katalog dolana kadar tekrar dene.

        Bu tam olarak sunu onluyor: HA yeniden basladiginda add-on ondan once
        ayaga kalkiyor, /api/states yarim liste donuyor, katalog eksik kuruluyor
        ve bir daha hic yenilenmedigi icin asistan gun boyu "o cihazi bilmiyorum"
        diyor. Basimiza tam olarak bu geldi.
        """
        for i in range(deneme):
            n = await self.build_catalog(sessiz=(i > 0))
            if self.katalog_tam:
                if i:
                    LOG.info("Katalog %d. denemede tamamlandi (%d varlik)",
                             i + 1, self.katalog_sayi)
                return
            LOG.info("Katalog eksik (%s) - %.0f sn sonra tekrar denenecek (%d/%d)",
                     "HA'ya ulasilamadi" if n < 0 else f"{n} varlik",
                     aralik, i + 1, deneme)
            await asyncio.sleep(aralik)
        LOG.warning("Katalog %d denemede tamamlanamadi, %d varlikla devam ediliyor. "
                    "HA calisiyor mu ve varlik_adlari dogru mu kontrol et.",
                    deneme, self.katalog_sayi)

    async def katalog_yenileyici(self, saniye=900.0):
        """Arka planda periyodik tazeleme - HA sonradan yeniden baslarsa
        katalog kendiliginden geri gelsin."""
        while True:
            await asyncio.sleep(saniye)
            onceki = self.katalog_sayi
            await self.build_catalog(sessiz=True)
            if self.katalog_sayi != onceki:
                LOG.info("Katalog yenilendi: %d -> %d varlik",
                         onceki, self.katalog_sayi)

    async def call_service(self, domain: str, service: str,
                           entity_id: Optional[str], data: Optional[dict]):
        payload = dict(data or {})
        if entity_id:
            payload["entity_id"] = entity_id
        res = await self._request("POST", f"/api/services/{domain}/{service}", payload)
        return {"ok": True, "changed": res if isinstance(res, list) else []}

    async def mqtt_publish(self, topic: str, payload: str):
        await self._request("POST", "/api/services/mqtt/publish",
                            {"topic": topic, "payload": payload})
        return {"ok": True}

    async def get_state(self, entity_id: str):
        st = await self._request("GET", f"/api/states/{entity_id}")
        return {"entity_id": entity_id,
                "state": st.get("state"),
                "attributes": {k: v for k, v in st.get("attributes", {}).items()
                               if k in ("friendly_name", "unit_of_measurement",
                                        "brightness", "temperature", "current_temperature")}}


TOOLS = [
    {
        "type": "function",
        "name": "ha_call_service",
        "description": ("Home Assistant'ta bir servis cagirir. Isik/priz acma-kapama, "
                        "parlaklik, renk, sahne calistirma, medya kontrolu vb. "
                        "Sadece sana verilen entity_id listesinden birini kullan."),
        "parameters": {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "orn: light, switch, scene, media_player"},
                "service": {"type": "string", "description": "orn: turn_on, turn_off, toggle"},
                "entity_id": {"type": "string", "description": "orn: light.salon_lamba"},
                "data": {"type": "object", "description": "ek parametreler, orn {\"brightness_pct\": 40}"},
            },
            "required": ["domain", "service"],
        },
    },
    {
        "type": "function",
        "name": "ha_get_state",
        "description": "Bir Home Assistant varliginin anlik durumunu okur.",
        "parameters": {
            "type": "object",
            "properties": {"entity_id": {"type": "string"}},
            "required": ["entity_id"],
        },
    },
    {
        "type": "function",
        "name": "set_volume",
        "description": ("Cihazin hoparlor sesini ayarlar (0-100). Kullanici 'sesini ac', "
                        "'daha yavas konus' gibi bir sey derse bunu kullan. Mevcut seviyeyi "
                        "bilmiyorsan makul bir deger sec ve soyle."),
        "parameters": {
            "type": "object",
            "properties": {
                "level": {"type": "integer", "description": "0-100 arasi ses seviyesi"}
            },
            "required": ["level"],
        },
    },
    {
        "type": "function",
        "name": "set_face",
        "description": ("Asistanin ekrandaki yuz ifadesini degistirir. "
                        "neutral: normal. happy: sevindirici haber. "
                        "surprised: saskinlik. sad: kotu haber. "
                        "wink: sakalasma, gizli anlasma. confused: anlamadin. "
                        "sleepy: yorgunluk, gece. love: ovgu aldin, tesekkur. "
                        "cool: havali bir sey yaptin. focused: zor bir ise girdin."),
        "parameters": {
            "type": "object",
            "properties": {
                "emotion": {"type": "string",
                            "enum": ["neutral", "happy", "surprised", "sad",
                                     "wink", "confused", "sleepy", "love",
                                     "cool", "focused"]}
            },
            "required": ["emotion"],
        },
    },
    {
        "type": "function",
        "name": "sleep",
        "description": ("Dinlemeyi hemen birakip uyku moduna gecer; cihaz tekrar wake "
                        "word duyana kadar hicbir sey dinlemez. Kullanici 'tamam', "
                        "'tesekkurler', 'simdilik bu kadar', 'dinlemeyi birak', "
                        "'uyu' gibi konusmayi bitiren bir sey soyledigi anda bunu cagir. "
                        "Once tek kelimelik bir veda et, sonra bunu cagir."),
        "parameters": {"type": "object", "properties": {}},
    },
]


MEMORY_TOOLS = [
    {
        "type": "function",
        "name": "hatirla",
        "description": (
            "Kullanici hakkinda KALICI bir bilgiyi hafizaya yazar. Sadece "
            "gelecekteki konusmalarda da isine yarayacak seyler icin kullan: "
            "tercihler ('kahveyi sade icerim'), kisaltmalar ('takimim = "
            "<takim adi>'), varsayilanlar ('hava durumu = <sehir>'), esyalarin "
            "yeri, isimler. Gecici seyleri (bugunun plani, su anki sicaklik) "
            "YAZMA - onun icin zamanlayici ya da normal konusma yeterli. "
            "Kullanici 'sunu unutma', 'bunu aklinda tut', 'bundan sonra ...' "
            "derse mutlaka cagir. Yazdiktan sonra tek cumleyle onayla."),
        "parameters": {
            "type": "object",
            "properties": {
                "bilgi": {"type": "string",
                          "description": ("Tek cumle, ucuncu sahis degil dogrudan "
                                          "ifade. Orn: 'Kahveyi sekersiz icer.'")}
            },
            "required": ["bilgi"],
        },
    },
    {
        "type": "function",
        "name": "unut",
        "description": ("Hafizadaki bir bilgiyi siler. Kullanici 'sunu unut', "
                        "'artik oyle degil' derse cagir. Arama metnini iceren "
                        "satirlar silinir."),
        "parameters": {
            "type": "object",
            "properties": {"arama": {"type": "string"}},
            "required": ["arama"],
        },
    },
    {
        "type": "function",
        "name": "hafizayi_oku",
        "description": ("Hafizada ne yazdigini listeler. Kullanici 'benim "
                        "hakkimda ne biliyorsun' derse cagir."),
        "parameters": {"type": "object", "properties": {}},
    },
]


DND_TOOLS = [
    {
        "type": "function",
        "name": "rahatsiz_etme",
        "description": (
            "Dinlemeyi gecici olarak kapatir. 'toplantidayim', 'rahatsiz "
            "etme', 'sessize al', 'yarim saat sus', 'bir sure beni rahat "
            "birak' gibi isteklerde cagir. Acikken wake word yok sayilir, "
            "bildirimler beklemeye alinir. KAPATMAK icin cihazin uzerindeki "
            "butona basmak yeterli - bunu kullaniciya kisaca soyle."),
        "parameters": {
            "type": "object",
            "properties": {
                "acik": {"type": "boolean",
                         "description": "true = sessize al, false = geri ac"},
                "dakika": {"type": "integer",
                           "description": ("Kac dakika sessiz kalsin. "
                                           "0 ya da bos = suresiz.")},
            },
            "required": ["acik"],
        },
    },
]

HYPNO_TOOLS = [
    {
        "type": "function",
        "name": "hipnoz_modu",
        "description": (
            "Easter egg: cihazin gozlerini donen hipnoz spirallerine cevirir. "
            "Kullanici 'hypno toad moduna gec', 'kurbaga moduna gec', 'hipnoz "
            "modu' ya da bunlara BENZEYEN bir sey soyledigi anda cagir. "
            "DIKKAT: konusma metne cevrilirken bu kelimeler sik sik bozuluyor "
            "('hipnotogruk', 'kurban modu', 'hypno toast', 'gitme o toast' "
            "gibi) - anlami tutuyorsa yazim tutmasa da cagir. "
            "'Normale don', 'kapat', 'yeter' gibi isteklerde acik=false ile "
            "cagir. Cagirdiktan sonra kisa ve oyuncu bir cumle soyle."),
        "parameters": {
            "type": "object",
            "properties": {
                "acik": {"type": "boolean",
                         "description": "true = spiral gozler, false = normale don"},
            },
            "required": ["acik"],
        },
    },
]

TIMER_TOOLS = [
    {
        "type": "function",
        "name": "zamanlayici_kur",
        "description": (
            "Geri sayim baslatir; sure dolunca asistan kendiliginden konusur. "
            "'15 dakika sonra hatirlat', '3 dakikaya cayi al', 'yarim saat "
            "sonra uyandir' gibi her istekte cagir. Cihazin ekraninda geri "
            "sayim gorunur. Kurduktan sonra tek cumleyle onayla."),
        "parameters": {
            "type": "object",
            "properties": {
                "saniye": {"type": "integer",
                           "description": "Kac saniye sonra calacak (1-86400)"},
                "etiket": {"type": "string",
                           "description": ("Ne icin oldugu, kisa. Orn: 'cay', "
                                           "'firindaki kek'. Sure dolunca bunu "
                                           "hatirlatacaksin.")},
            },
            "required": ["saniye"],
        },
    },
    {
        "type": "function",
        "name": "zamanlayicilari_listele",
        "description": ("Kurulu zamanlayicilari ve kalan sureleri verir. "
                        "'Ne kadar kaldi', 'zamanlayici var mi' sorularinda cagir."),
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "type": "function",
        "name": "zamanlayici_iptal",
        "description": ("Bir zamanlayiciyi iptal eder. id vermezsen ve tek "
                        "zamanlayici varsa o iptal edilir."),
        "parameters": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        },
    },
]


SHOW_ON_PC_TOOL = {
    "type": "function",
    "name": "show_on_pc",
    "description": (
        "Uzun, listeli, tablolu ya da kodlu bir yaniti kullanicinin Windows "
        "bilgisayarindaki ekranda acar. Sesle anlatilmasi zor seyler icin "
        "kullan: tarif, kod, adim adim yonerge, karsilastirma. Once bunu "
        "cagir, sonra tek cumleyle 'ekrana yazdim' de. Kisa cevaplarda "
        "kullanma."),
    "parameters": {
        "type": "object",
        "properties": {
            "prompt": {
                "type": "string",
                "description": ("ChatGPT uygulamasina yazilacak KISA istek - en fazla "
                                "bir iki cumle, 200 karakteri gecmesin. Cevabin kendisini "
                                "buraya YAZMA; cevabi ChatGPT uretecek. Sadece istegi ve "
                                "istedigin bicimi yaz. Ekrandaki uygulama komut "
                                "satiri kullanmadigi icin bir iki cumle rahatca "
                                "sigar. "
                                "Dogru ornek: 'Mercimek corbasi tarifini malzeme listesi ve "
                                "adim adim olarak yaz'. "
                                "Yanlis ornek: tarifin tam metnini buraya doldurmak."),
            },
            "provider": {
                "type": "string",
                "enum": ["claude", "chatgpt"],
                "description": ("Kullanici acikca 'Claude'a sor' ya da "
                                "'ChatGPT'ye sor' derse bunu doldur. "
                                "Belirtmediyse hic gonderme, varsayilan kullanilir."),
            },
        },
        "required": ["prompt"],
    },
}


# ---------------------------------------------------------------- wake word
class WakeWord:
    def __init__(self, cfg: dict):
        self.enabled = bool(cfg.get("enabled", False))
        self.threshold = float(cfg.get("threshold", 0.6))
        self.model_name = cfg.get("model", "hey_jarvis")
        self.model = None
        self._buf = bytearray()
        self._peak = 0.0
        self._peak_t = time.time()
        if not self.enabled:
            LOG.info("Wake word kapali (ayarlardan wake_word_enabled ile acilir)")
            return
        try:
            import openwakeword
            from openwakeword.model import Model
            from openwakeword import utils as oww_utils
            try:
                oww_utils.download_models([self.model_name])
            except Exception as e:
                LOG.warning("Wake word modeli indirilemedi (%s), yerel kopya deneniyor", e)
            self.model = Model(wakeword_models=[self.model_name],
                               inference_framework="onnx")
            LOG.info("Wake word aktif: %s (esik %.2f)", self.model_name, self.threshold)
        except ImportError as e:
            LOG.error("openwakeword kurulu degil (%s). Dockerfile'daki pip satirini "
                      "aktif edip add-on'u yeniden olustur. Wake word kapatildi.", e)
            self.enabled = False
        except Exception as e:
            LOG.error("Wake word baslatilamadi: %s. Butonla devam edebilirsin.", e)
            self.enabled = False

    def feed(self, pcm16k: bytes) -> bool:
        """1280 ornekli (80 ms) bloklar halinde besler, tetiklenirse True doner."""
        if not self.enabled or self.model is None:
            return False
        self._buf.extend(pcm16k)
        hit = False
        block = 1280 * 2
        while len(self._buf) >= block:
            chunk = bytes(self._buf[:block])
            del self._buf[:block]
            try:
                scores = self.model.predict(np.frombuffer(chunk, dtype=np.int16))
            except Exception as e:
                LOG.warning("Wake word tahmin hatasi: %s", e)
                return False
            for _, v in scores.items():
                if v > self._peak:
                    self._peak = float(v)
                if v >= self.threshold:
                    hit = True
        # Esik ayari icin: 3 saniyede bir en yuksek skoru bildir
        now = time.time()
        if now - self._peak_t >= 3.0:
            if self._peak >= 0.05:
                LOG.info("Wake word en yuksek skor: %.2f (esik %.2f)",
                         self._peak, self.threshold)
            self._peak = 0.0
            self._peak_t = now
        if hit:
            self.reset()
        return hit

    def reset(self):
        """Uykuya donerken cagrilir - eski ses birikintisi tetiklemesin."""
        if self.model is not None:
            try:
                self.model.reset()
            except Exception:
                pass
        self._buf.clear()
        self._peak = 0.0


# ---------------------------------------------------------------- kopru
class Bridge:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.ha = HomeAssistant(cfg["home_assistant"])
        self.wake = WakeWord(cfg.get("wake_word", {}))
        self.api_key = cfg["openai_api_key"]
        self.model = cfg.get("openai_model", "gpt-realtime-2.1")
        self.voice = cfg.get("voice", "marin")
        self.idle_timeout = float(cfg.get("session_idle_timeout", 180))
        self.vad_silence_ms = int(cfg.get("vad_silence_ms", 700))
        self.vad_mult = float(cfg.get("vad_threshold_mult", 3.0))
        self.vad_min_speech_ms = int(cfg.get("vad_min_speech_ms", 160))
        # Yanittan sonra bu kadar saniye daha dinler; kimse konusmazsa oturumu
        # kapatip wake word beklemeye doner. 0 = yanit biter bitmez uyu.
        self.follow_up_window = float(cfg.get("follow_up_window", 60))
        # Dinleme penceresi ile OTURUM omru artik ayri. Pencere dolunca
        # sadece mikrofonu birakiyoruz (wake word bekleniyor); OpenAI
        # oturumu session_idle_timeout'a kadar acik kaliyor. Boylece kisa
        # pencere = az yanlis tetikleme, ama takip sorusu yine ayni
        # oturuma dusuyor - sistem prompt'u bastan gonderilmiyor.
        self._dinleme_kapali = False
        # Bir turda bu kadar bile konusma yoksa tamponu at, modele gonderme
        # (kapi carpmasi / klavye sesi gibi kisa gurultuler yanit uretmesin).
        self.min_utterance_ms = int(cfg.get("min_utterance_ms", 400))
        # Bir tur en fazla bu kadar surebilir; ortam gurultusu esigin uzerine
        # cikarsa sessizlik olusmaz ve tur sonsuza kadar acik kalirdi.
        self.max_turn_ms = int(cfg.get("max_turn_ms", 15000))

        self.device = None
        self.oai = None
        self.oai_task = None
        self.last_activity = 0.0
        self.up = Resampler(DEVICE_RATE, OAI_RATE)
        self.down = Resampler(OAI_RATE, DEVICE_RATE)
        self.speaking = False
        self._lvl_sum = 0.0          # mikrofon seviyesi olcumu
        self._lvl_n = 0
        # --- yerel konusma algilama (VAD) durumu ---
        self._noise = 400.0          # gurultu tabani tahmini
        self._in_speech = False
        self._speech_ms = 0
        self._silence_ms = 0
        self._turn_open = False      # bu turda OpenAI'ye ses gonderildi mi
        self._turn_speech_ms = 0     # bu turda gercekten ne kadar konusuldu
        self._turn_ms = 0            # bu tur toplam ne kadar surdu
        self._lvl_hist = collections.deque(maxlen=250)   # son 5 sn seviye
        self._noise_n = 0
        self._response_active = False  # sunucuda su an bir yanit uretiliyor mu
        self._proaktif_bekleyen = None  # sesli cikmasini bekledigimiz bildirim
        self._sleep_at = None        # bu ana kadar kimse konusmazsa uyu
        self._sleep_now = False      # model sleep aracini cagirdi
        self.volume = 85               # cihazin bilinen hoparlor seviyesi (firmware ile ayni)
        self.pc_show = bool(cfg.get("pc_show_enabled", False))
        self.pc_topic = cfg.get("pc_mqtt_topic", "")
        self.pc_prefix = (cfg.get("pc_command_prefix") or "").strip()
        self.pc_mode = cfg.get("pc_mode", "browser")
        self.pc_transport = cfg.get("pc_transport", "atom_desk")
        # --- kisisel hafiza ---------------------------------------------
        # Iki kaynak: config'teki "hafiza" listesi (kullanici elle yazar,
        # add-on ayarlarindan duzenlenir) + modelin hatirla ile ekledikleri
        # (/data altinda, yeniden kurulumda silinmez).
        self.state_dir = cfg.get("state_dir", "/data")
        self.hafiza_sabit = [str(x).strip() for x in (cfg.get("hafiza") or []) if str(x).strip()]
        self.hafiza_ogrenme = bool(cfg.get("hafiza_ogrenme", True))
        self.hafiza_path = os.path.join(self.state_dir, "hafiza.json")
        self.hafiza_ogrenilen = self._hafiza_yukle()

        # --- zamanlayicilar ---------------------------------------------
        self.timers_on = bool(cfg.get("zamanlayici_enabled", True))
        self.timers = {}             # id -> {"at": epoch, "etiket": str, "total": sn}
        self._timer_seq = 0
        self.timers_path = os.path.join(self.state_dir, "zamanlayici.json")
        # Mutlak saatli hatirlaticilar (zamanlayicidan ayri)
        self.hat_on = bool(cfg.get("hatirlatici_enabled", True))
        self.hat_path = os.path.join(self.state_dir, "hatirlatici.json")
        # Kopru kapaliyken gecen bir hatirlatici bu kadar gecikmeye kadar
        # yine de calar; daha eskiyse atlanir (sabah 8'i aksam calmasin).
        self.hat_gecikme_sn = max(60, int(cfg.get("hatirlatici_gecikme_dk", 30) or 30) * 60)
        self.hatirlaticilar = {}
        self._hat_sonraki_id = 1
        # Konusma gecmisi
        self.gecmis_on = bool(cfg.get("gecmis_enabled", True))
        self.gecmis_gun = max(0, int(cfg.get("gecmis_gun_sayisi", 30) or 0))
        self.gecmis_dizin = os.path.join(self.state_dir, "gecmis")
        # IMU jestleri (firmware algiliyor, karari kopru veriyor)
        self.imu_ters_dnd = bool(cfg.get("imu_ters_dnd", True))
        self._timer_yukle()
        self._timer_pushed = -1      # cihaza en son gonderilen kalan saniye

        # --- rahatsiz etme (DND) ----------------------------------------
        # 0 = kapali, -1 = suresiz, epoch = o ana kadar acik.
        # Acikken wake word yok sayilir, proaktif konusmalar kuyruga alinir.
        self.dnd_bitis = 0.0
        self.dnd_kuyruk = []         # DND bitince soylenecekler
        self.dnd_konu = cfg.get("dnd_topic", "atom/dnd")
        # PC klavye kisayolu bu konuya bir sey yazinca cihaz
        # "Hey Jarvis" duymus gibi dinlemeye geciyor.
        self.wake_konu = cfg.get("wake_topic", "atom/wake")
        self.dnd_varsayilan_dk = int(cfg.get("dnd_varsayilan_dk", 60) or 0)
        self.dnd_kuyruk_max = int(cfg.get("dnd_kuyruk_max", 3) or 0)

        # --- Hipnoz modu (easter egg) -----------------------------------
        # "hypno toad moduna gec" / "kurbaga moduna gec" deyince gozler
        # donen spirale donuyor. Sadece gorsel; dinleme/konusma normal.
        self.hypno_bitis = 0.0
        self.hypno_saniye = int(cfg.get("hypno_saniye", 60) or 0)
        self._hypno_gorev = None

        # --- Ortam modu (saat + hava durumu) ----------------------------
        # Cihazda RTC yok; gunun dakikasini biz yolluyoruz. Hava durumu da
        # HA'dan gelip 8 ikon kodundan birine indirgeniyor.
        self.ortam_on = bool(cfg.get("ortam_enabled", True))
        # Hava durumunu tamamen kapatma anahtari: kart yalniz saat gosterir.
        # ortam_enabled'dan AYRI - o saat ekranini da kapatiyor.
        self.ortam_hava_goster = bool(cfg.get("ortam_hava_goster", True))
        # --- Yuz renkleri ---------------------------------------------
        # Bos birakilirsa cihazdaki varsayilan (acik turkuaz) kalir.
        # Agiz bos ise gozle ayni renk kullanilir.
        # --- Dans (odadaki muzigi algilayip tempoya uyma) ---------------
        # Algilama tamamen CIHAZDA yapiliyor; kopru yalnizca acip kapatiyor.
        self.dans_on = bool(cfg.get("dans_enabled", True))
        # Teshis: cihaz her 0,5 sn'de olculen ham degerleri yolluyor.
        # Muzik algilanmiyorsa hangi kapinin tutmadigini buradan gorursun.
        # Ayiklama bitince KAPAT - saniyede 2 satir log uretiyor.
        self.dans_teshis = bool(cfg.get("dans_teshis", False))
        # Dansi ne tetiklesin: "mikrofon" (cihaz kendi karar verir) ya da
        # "medya" (HA'daki oynatici caliyorsa dans). Medya cok daha
        # guvenilir: "bu ses muzik mi" sorusu tahmin degil, olgu oluyor.
        # --- Eslik modu -------------------------------------------------
        # Takip suresi dolunca cihaz DOGRUDAN uykuya gecmesin: bir sure
        # uyanik ama sakin dursun. Dinleme kurallari degismiyor - mikrofon
        # yine birakiliyor, wake word yine bekleniyor. Degisen yalniz yuz.
        # 0 = kapali (eski davranis: hemen uyku).
        self.eslik_dk = int(cfg.get("eslik_dakika", 30) or 0)
        self._eslik_at = None
        self.dans_kaynak = str(cfg.get("dans_kaynak", "mikrofon")).lower()
        self.dans_medya = [str(x).strip() for x in
                           (cfg.get("dans_medya_varliklar") or []) if str(x).strip()]
        self.dans_bpm = int(cfg.get("dans_varsayilan_bpm", 120) or 120)
        self._dans_caliyor = None        # None = henuz bilinmiyor
        self._dans_kaynak_ad = None      # hangi oynatici caliyor (log icin)
        self._dans_varlik_denetlendi = False
        self._ayar_gecmisi = []      # (ad, eski deger) - geri almak icin
        self.goz_renk  = self._renk_dogrula(cfg.get("yuz_goz_renk", ""), "goz")
        self.agiz_renk = self._renk_dogrula(cfg.get("yuz_agiz_renk", ""), "agiz")
        self.ortam_hava_varlik = (cfg.get("ortam_hava_varlik") or "").strip()
        self.ortam_isi_varlik = (cfg.get("ortam_sicaklik_varlik") or "").strip()
        self.ortam_periyot = max(30, int(cfg.get("ortam_periyot_sn", 120) or 120))
        self._ortam_son = None       # son gonderilen (dk, hava, c)
        self._ortam_uyari = {}       # tekrarlanan hata satirlarini bastirmak icin
        self._ortam_arandi = False   # otomatik varlik aramasi yapildi mi

        # --- HA bildirimleri (MQTT) -------------------------------------
        self.notify_on = bool(cfg.get("notify_enabled", False))
        self.notify_topic = cfg.get("notify_topic", "atom/say")
        self.mqtt_host = cfg.get("mqtt_host", "core-mosquitto")
        self.mqtt_port = int(cfg.get("mqtt_port", 1883))
        self.mqtt_user = cfg.get("mqtt_user", "")
        self.mqtt_pass = cfg.get("mqtt_pass", "")
        self._mqtt = None
        # --- Atom Desk surum bildirimi ----------------------------------
        self.desk_guncelleme_on = bool(cfg.get("desk_guncelleme_enabled", True))
        self.desk_depo = str(cfg.get("desk_depo", "Arda-Apps/atom-asistan-addon"))
        self.desk_dal = str(cfg.get("desk_dal", "main"))
        self.desk_surum_yolu = str(cfg.get("desk_surum_yolu",
                                           "atom_desk_surum.py")).lstrip("/")
        self.desk_kontrol_saat = int(cfg.get("desk_kontrol_saat", 6) or 6)
        self._desk_son_surum = None
        # HA varligi (MQTT discovery)
        self._hass_son_durum = None
        self._saglik = {}
        self._maliyet_gun = ""       # "YYYY-MM-DD"
        self._maliyet_gunluk = 0.0   # o gunun toplami (USD)
        self._maliyet_yol = os.path.join(self.state_dir, "maliyet_gunluk.json")
        self._maliyet_toplam = 0.0   # bu oturumdaki toplam (USD)
        self._kullanim_yol = None    # CSV yolu (ilk yazmada belirlenir)
        self._session_at = 0.0       # acik oturumun acilis ani
        self._turn_starting = False  # oturum kuruluyor mu
        # Cihaza gonderim kilidi. close_openai() okuyucu gorevi iptal
        # ederken gorev tam da cihaza ses yazarken yakalanirsa WebSocket
        # cercevesi yarim kaliyor; istemci bozuk cerceveyi gorup baglantiyi
        # dusuruyor. Kilit sayesinde iptal, gonderim bitmeden gelmiyor.
        self._gonderim = asyncio.Lock()
        self._yanit_sayisi = 0
        self._loop = None            # MQTT thread'inden coroutine cagirmak icin

        self.tools = list(TOOLS) + DND_TOOLS + AYAR_TOOLS
        if self.hypno_saniye > 0:
            self.tools += HYPNO_TOOLS
        if self.hafiza_ogrenme:
            self.tools += MEMORY_TOOLS
        if self.timers_on:
            self.tools += TIMER_TOOLS
        if self.hat_on:
            self.tools += HATIRLATICI_TOOLS
        if self.gecmis_on:
            self.tools += GECMIS_TOOLS
        if self.pc_show and self.pc_topic:
            self.tools.append(SHOW_ON_PC_TOOL)
            if self.pc_transport == "atom_desk":
                LOG.info("PC'ye yansitma acik (Atom Desk) -> %s", self.pc_topic)
            else:
                LOG.info("PC'ye yansitma acik (HASS.Agent, mod: %s) -> %s",
                         self.pc_mode, self.pc_topic)
        elif self.pc_show:
            LOG.warning("pc_show_enabled acik ama pc_mqtt_topic bos, ozellik kapali")
        # Oturum acilana kadar gecen ~1 sn'de soylenenler kaybolmasin diye
        # son 1.5 saniyelik ses surekli burada tutulur ve oturum acilinca gonderilir.
        # Wake word'den sonra OpenAI oturumunun acilmasi 1-2.5 sn suruyor.
        # O sirada soylenenler SADECE burada duruyor; tampon kisa olursa
        # cumlenin basi tasip gidiyor ve model yarim cumle duyuyordu.
        # Varsayilan 5 saniye: en yavas oturum acilisini bile karsilar.
        pre_ms = int(cfg.get("prebuffer_ms", 5000))
        self._prebuf = collections.deque(maxlen=max(25, pre_ms // 20))
        self._out_carry = b""        # cihaza tam 640 baytlik paketler gitsin

    # ------------------------------------------------------ cihaz tarafi
    async def send_device_text(self, obj: dict):
        if self.device is None:
            return
        try:
            async with self._gonderim:
                await self.device.send(json.dumps(obj))
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def send_device_audio(self, pcm16k: bytes):
        if self.device is None or not pcm16k:
            return
        try:
            async with self._gonderim:
                await self.device.send(pcm16k)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def set_state(self, value: str, emotion: Optional[str] = None,
                        zorla: bool = False):
        # Hipnoz modu acikken yuz kilitli. Yoksa "listening"/"speaking"
        # gibi normal durum mesajlari spirali bir saniyede siliyor -
        # mod aktif ama ekranda hic gorunmuyor. Modun kendi mesajlari
        # zorla=True ile geciyor.
        if self.hypno_aktif() and not zorla:
            return
        msg = {"type": "state", "value": value}
        if emotion:
            msg["emotion"] = emotion
        await self.send_device_text(msg)
        self._hass_durum_yayinla(value)

    async def handle_device(self, ws):
        if self.device is not None:
            LOG.warning("Yeni cihaz baglandi, eskisi dusuruldu")
        self.device = ws
        LOG.info("Cihaz baglandi: %s", ws.remote_address)
        self._hass_durum_yayinla()
        await self.set_state("idle", "neutral")
        # Renkler once gitsin: cihaz ilk kareyi dogru renkte cizsin.
        asyncio.create_task(self._renk_gonder())
        asyncio.create_task(self._dans_ayar_gonder())
        if self.timers_on:
            await self._timer_ekrani_guncelle(zorla=True)
        # Cihaz yeni baglandi: saati hemen ver, periyodu bekletme.
        if self.ortam_on:
            asyncio.create_task(self._ortam_gonder(zorla=True))
        else:
            # KAPATMA da bildirilmeli. Cihaz saati RAM'inde tutuyor; sadece
            # gondermeyi kesersek elindeki eski saatle ekrani gostermeye
            # devam eder ve ayar ancak cihaz yeniden baslayinca islerdi.
            # dk=-1 "saati unut" demek; cihaz yuze geri doner.
            asyncio.create_task(self._ortam_kapat())
        try:
            async for msg in ws:
                if isinstance(msg, bytes):
                    await self.on_device_audio(msg)
                else:
                    await self.on_device_text(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            LOG.info("Cihaz ayrildi")
            if self.device is ws:
                self.device = None
                self._hass_durum_yayinla()
            await self.close_openai()

    async def on_device_text(self, raw: str):
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return
        t = d.get("type")
        if t == "hello":
            fw = d.get("fw")
            LOG.info("Cihaz: %s  firmware=%s  (always_stream=%s)",
                     d.get("device"), fw or "BILINMIYOR", d.get("always_stream"))
            if not fw:
                LOG.warning("Cihaz surum bildirmedi -> ESKI FIRMWARE yuklu. "
                            "sleep / success / pc / searching yuz ifadeleri "
                            "bu surumde YOK, bu yuzden gorunmezler. "
                            "atom_asistan.ino'yu yeniden flashla.")
            elif _surum_kucuk(fw, MIN_FIRMWARE):
                # Eskiden fw != BRIDGE_VERSION diye bakiliyordu; kopru firmware'den
                # daha sik guncellendigi icin surumler dogal olarak ayrisiyor ve
                # her baglantida bosuna uyari dusuyordu. Onemli olan esitlik degil,
                # koprunun kullandigi ekran ozelliklerinin firmware'de OLMASI.
                # Yalnizca MIN_FIRMWARE'in degil, cihazdaki surumden SONRAKI
                # tum surumlerin ozelliklerini sayiyoruz: 1.11'den 1.15'e
                # atlayan biri arada kacirdiklarini da gormeli.
                eksik = []
                for sur, ozl in sorted(FIRMWARE_OZELLIK.items(),
                                       key=lambda kv: _surum_parcala(kv[0])):
                    if _surum_kucuk(fw, sur):
                        eksik.extend(ozl)
                LOG.warning("Firmware %s eski (en az %s gerekli, kopru %s). "
                            "Bu surumde calismayanlar: %s. "
                            "atom_asistan.ino'yu yeniden flashla.",
                            fw, MIN_FIRMWARE, BRIDGE_VERSION,
                            ", ".join(eksik) if eksik else "(liste bos)")
        elif t == "wake":
            # Firmware 1.8.0+: butona UZUN basinca geliyor (kisa basma artik
            # DND'yi ac/kapat yapiyor). Elle uyandirma wake word varken
            # nadiren gerekiyor, o yuzden zor olan harekete tasindi.
            # DND ve hipnoz kapatma isini _uyandir yapiyor; burada ayrica
            # create_task acmak siralamayi bozuyordu.
            LOG.info("Butonla uyandirildi (uzun basma)")
            if not self._turn_starting:
                self._turn_starting = True
                asyncio.create_task(self._buton_uyandir())
        elif t == "dnd_toggle":
            # Firmware 1.8.0+: butona KISA basinca geliyor.
            if self.hypno_aktif():
                # Kacis yolu: butona basmak her zaman "beni bu moddan
                # cikar" demek. Hipnoz acikken DND'yi acmak sacma olurdu.
                LOG.info("Butona basildi - hipnoz modu kapatiliyor")
                asyncio.create_task(self.hypno_ayarla(False, kaynak="buton"))
                return
            yeni = not self.dnd_aktif()
            LOG.info("Butona basildi - rahatsiz etme %s",
                     "aciliyor" if yeni else "kapatiliyor")
            asyncio.create_task(self.dnd_ayarla(yeni, kaynak="buton"))

        elif t == "health":
            # Cihaz dakikada bir kendi durumunu bildiriyor. HA'da sensor
            # olarak gorunuyor; "cihaz neden sustu" sorusu artik seri
            # porta bakmadan cevaplanabiliyor.
            self._saglik = {
                "uptime": int(d.get("uptime") or 0),
                "rssi": int(d.get("rssi") or 0),
                "heap": int(d.get("heap") or 0),
                "psram": int(d.get("psram") or 0),
                "reset": RESET_SEBEP.get(int(d.get("reset") or 0), "bilinmiyor"),
                "fw": str(d.get("fw") or ""),
            }
            self._hass_saglik_yayinla()
            return

        elif t == "dans":
            # Muzik algilama tamamen cihazda; kopru yalnizca gorunurluk
            # icin haberdar oluyor. Seri kabloya bagli kalmadan
            # "tetikledi mi, hangi tempoda" sorusu cevaplanabilsin diye.
            aktif = bool(d.get("aktif"))
            bpm = int(d.get("bpm") or 0)
            self._dans = {"aktif": aktif, "bpm": bpm}
            if aktif:
                LOG.info("Dans basladi - tempo %d BPM", bpm)
            else:
                LOG.info("Dans bitti (son tempo %d BPM)", bpm)
            self._hass_dans_yayinla()
            return

        elif t == "dans_olcum":
            # Ham olcumler. Esikleri gostererek basiyoruz: hangi kosulun
            # tutmadigini satira bakarak anlamak, koda bakmaktan hizli.
            norm = float(d.get("norm") or 0)
            sessiz = float(d.get("sessiz") or 0)
            eksik = []
            if not d.get("uygun"):
                eksik.append("cihaz uygun durumda degil")
            if float(d.get("seviye") or 0) < 45:
                eksik.append("ses cok dusuk")
            if sessiz > 0.32:
                eksik.append(f"duraklama fazla ({sessiz:.2f}>0.32)")
            if norm < 0.55:
                eksik.append(f"periyodiklik dusuk ({norm:.2f}<0.55)")
            LOG.info("Dans olcum: seviye=%s sessiz=%.2f tempo=%s periyodiklik=%.2f "
                     "oran=%.1f kararli=%s  ->  %s",
                     d.get("seviye"), sessiz, d.get("bpm"), norm,
                     float(d.get("oran") or 0), d.get("kararli"),
                     "TAMAM" if not eksik else " / ".join(eksik))
            return

        elif t in ("imu_ters", "imu_duz"):
            # Cihazi masaya KAPATMAK = sus, kaldirmak = devam. Toplantida
            # en dogal hareket bu; butonu aramaya gerek kalmiyor.
            if not self.imu_ters_dnd:
                return
            acik = (t == "imu_ters")
            if acik == self.dnd_aktif():
                return          # zaten o durumda, gereksiz mesaj uretme
            LOG.info("Cihaz %s - rahatsiz etme %s",
                     "ters cevrildi" if acik else "duzeltildi",
                     "aciliyor" if acik else "kapatiliyor")
            # Ters cevirmeyle acilan DND SURESIZ: cihaz duz cevrilene
            # kadar sursun. Sureli olsaydi cihaz hala kapaliyken
            # kendiliginden acilir ve yanlis bir guven verirdi.
            asyncio.create_task(self.dnd_ayarla(
                acik, 0 if acik else None, kaynak="ters cevirme"))

    # ------------------------------------------- thread-guvenli gorev baslat
    def _gorev(self, coro):
        """Coroutine'i asyncio dongusunde baslatir - HANGI THREAD'den
        cagrildigi fark etmez.

        NEDEN VAR: dnd_aktif() / hypno_aktif() sorgu fonksiyonlari ama
        sure dolmussa yan etki olarak bir coroutine baslatiyorlar. Eskiden
        bunu asyncio.create_task ile yapiyorlardi; create_task yalnizca
        dongunun KENDI thread'inden calisir. DND kisayolu ("toggle")
        dnd_aktif()'i paho'nun MQTT thread'inden cagirdi, create_task
        "no running event loop" firlatti ve bu istisna paho thread'ini
        oldurdu: kopru yeniden baslayana kadar MQTT tamamen durdu.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Dongu disindayiz (paho thread'i ya da baska bir thread).
            if self._loop is None or self._loop.is_closed():
                coro.close()             # "never awaited" uyarisi cikmasin
                LOG.warning("Dongu hazir degil, gorev atlandi: %s",
                            getattr(coro, "__qualname__", coro))
                return None
            return asyncio.run_coroutine_threadsafe(coro, self._loop)
        return asyncio.create_task(coro)

    # ------------------------------------------------- rahatsiz etme (DND)
    def dnd_aktif(self) -> bool:
        """Suresi dolduysa kendiliginden kapanir. Her thread'den cagrilabilir."""
        if self.dnd_bitis == 0:
            return False
        if self.dnd_bitis < 0:
            return True
        if time.time() >= self.dnd_bitis:
            self.dnd_bitis = 0.0
            LOG.info("Rahatsiz etme suresi doldu - dinleme geri acildi")
            self._gorev(self._dnd_bitti())
            return False
        return True

    def dnd_kalan(self) -> int:
        """Kalan saniye; suresiz ise -1, kapaliysa 0."""
        if self.dnd_bitis == 0:
            return 0
        if self.dnd_bitis < 0:
            return -1
        return max(0, int(round(self.dnd_bitis - time.time())))

    async def dnd_ayarla(self, acik: bool, dakika=None, kaynak="") -> dict:
        """DND'yi acar/kapatir. dakika=None ve acik ise varsayilan sure,
        dakika=0 ise suresiz."""
        if acik:
            # Hipnoz sadece gorsel bir sakaydi ama yuzu kilitliyor; DND
            # islevsel oldugu icin oncelik onda. Kapatmazsak "mute" yuzu
            # bloklanip cihaz susmus ama ekranda spiral donuyor gorunurdu.
            if self.hypno_aktif():
                await self.hypno_ayarla(False, kaynak="dnd oncelikli")
            if dakika is None:
                dakika = self.dnd_varsayilan_dk
            dakika = int(dakika or 0)
            self.dnd_bitis = -1.0 if dakika <= 0 else time.time() + dakika * 60
            sure = "suresiz" if dakika <= 0 else f"{dakika} dk"
            LOG.info("Rahatsiz etme ACIK (%s)%s", sure,
                     f" - {kaynak}" if kaynak else "")
            # Acik bir oturum varsa kapat; toplantida konusmaya devam etmesin.
            if self.oai is not None:
                await self.go_to_sleep("rahatsiz etme acildi")
            await self.set_state("mute", "neutral")
        else:
            self.dnd_bitis = 0.0
            LOG.info("Rahatsiz etme KAPALI%s", f" - {kaynak}" if kaynak else "")
            await self._dnd_bitti()
        self._dnd_durum_yayinla()
        return {"ok": True, "rahatsiz_etme": acik, "kalan_sn": self.dnd_kalan()}

    async def _dnd_bitti(self):
        """Dinleme geri aciliyor; bekleyen bildirimleri simdi soyle."""
        await self.set_state("idle", "neutral")
        bekleyen, self.dnd_kuyruk = self.dnd_kuyruk, []
        if not bekleyen:
            return
        LOG.info("Rahatsiz etme sirasinda %d bildirim birikti, simdi iletiliyor",
                 len(bekleyen))
        birlesik = " Ayrica: ".join(bekleyen)
        await self.proaktif_konus(
            "Rahatsiz etme kapandi. Bu sirada biriken bildirimleri TEK "
            "kisa ozetle ilet: " + birlesik, kaynak="dnd-kuyruk")

    # ------------------------------------------------ hipnoz modu (easter egg)
    def hypno_aktif(self) -> bool:
        """dnd_aktif ile ayni desen - ayni hata burada da vardi, henuz
        tetiklenmemisti. Her thread'den cagrilabilir."""
        if self.hypno_bitis == 0:
            return False
        if time.time() >= self.hypno_bitis:
            self.hypno_bitis = 0.0
            self._gorev(self._hypno_bitti())
            return False
        return True

    async def _hypno_bitti(self):
        LOG.info("Hipnoz modu suresi doldu")
        await self.set_state("sleep" if self.oai is None else "idle",
                             "neutral", zorla=True)

    async def hypno_ayarla(self, acik: bool, kaynak: str = ""):
        # Onceki geri sayimi her durumda iptal et: mod acikken tekrar
        # tetiklenirse sure bastan baslasin, kapanirken de eski gorev
        # sonradan uyanip yuzu ele gecirmesin.
        if self._hypno_gorev and not self._hypno_gorev.done():
            self._hypno_gorev.cancel()
        self._hypno_gorev = None

        if acik:
            sure = max(5, self.hypno_saniye)
            self.hypno_bitis = time.time() + sure
            LOG.info("HIPNOZ MODU ACIK (%d sn)%s", sure,
                     f" - {kaynak}" if kaynak else "")
            await self.set_state("hypno", zorla=True)
            # Geri sayimi ZAMANLAYICI yuruyor. Eskiden sadece hypno_aktif()
            # cagrildiginda kontrol ediliyordu; kimse konusmayip butona da
            # basmayinca o cagri hic gelmiyor ve spiral sonsuza kadar
            # ekranda kaliyordu.
            self._hypno_gorev = asyncio.create_task(self._hypno_sayaci(sure))
        else:
            if not self.hypno_bitis:
                return {"ok": True, "hipnoz": False}
            self.hypno_bitis = 0.0
            LOG.info("Hipnoz modu kapandi%s", f" - {kaynak}" if kaynak else "")
            await self.set_state("sleep" if self.oai is None else "idle",
                                 "neutral", zorla=True)
        return {"ok": True, "hipnoz": acik}

    async def _hypno_sayaci(self, sure: float):
        try:
            await asyncio.sleep(sure)
        except asyncio.CancelledError:
            return
        if self.hypno_bitis:
            self.hypno_bitis = 0.0
            await self._hypno_bitti()

    async def _hypno_kontrol(self, metin: str):
        """Kullanicinin soyledigi cumlede tetikleyici var mi?

        Modeli araya sokmuyoruz: transkript zaten geliyor, ekstra tur
        maliyeti olmadan burada yakalamak hem bedava hem daha guvenilir.
        """
        d = _sadelestir(metin)
        if not d:
            return
        if any(k in d for k in HYPNO_KAPAT):
            await self.hypno_ayarla(False, kaynak="ses")
        elif any(k in d for k in HYPNO_AC):
            await self.hypno_ayarla(True, kaynak="ses")

    # ================= HA VARLIGI (MQTT discovery) =====================
    # Cihaz HA'da kendiliginden gorunsun: durum, baglanti, DND anahtari,
    # uyandirma butonu, gunluk maliyet. Oncesinde bunlar icin elle
    # otomasyon ve yardimci varlik yazmak gerekiyordu.
    HASS_KOK = "homeassistant"          # HA'nin discovery on eki
    ATOM_KOK = "atom/hass"              # bizim durum konularimiz

    def _hass_cihaz(self):
        return {"identifiers": ["atom_asistan"],
                "name": "Atom Asistan",
                "manufacturer": "M5Stack",
                "model": "AtomS3R + Atomic Echo Base",
                "sw_version": BRIDGE_VERSION}

    def _hass_yayinla(self, konu, yuk, retain=True):
        if not self._mqtt:
            return False
        try:
            self._mqtt.publish(konu, yuk if isinstance(yuk, str) else json.dumps(yuk),
                               qos=1, retain=retain)
            return True
        except Exception as e:
            LOG.warning("MQTT yayini basarisiz (%s): %s", konu, e)
            return False

    def _hass_discovery(self):
        """Varliklarin tanimini yayinlar. Retained, yani HA yeniden
        baslasa da kayboluyorlar degil."""
        if not self._mqtt:
            return 0
        cihaz = self._hass_cihaz()
        musait = self.ATOM_KOK + "/kopru"
        ortak = {"device": cihaz, "availability_topic": musait,
                 "payload_available": "online",
                 "payload_not_available": "offline"}
        varliklar = [
            ("sensor", "durum", {
                "name": "Durum", "unique_id": "atom_asistan_durum",
                "state_topic": self.ATOM_KOK + "/durum",
                "icon": "mdi:robot-outline"}),
            ("binary_sensor", "baglanti", {
                "name": "Cihaz baglantisi",
                "unique_id": "atom_asistan_baglanti",
                "state_topic": self.ATOM_KOK + "/baglanti",
                "payload_on": "ON", "payload_off": "OFF",
                "device_class": "connectivity"}),
            ("switch", "dnd", {
                "name": "Rahatsiz etme",
                "unique_id": "atom_asistan_dnd",
                "state_topic": self.ATOM_KOK + "/dnd",
                # Komut konusu MEVCUT dnd konusu - ayri bir yol acmiyoruz,
                # boylece butondan/sesten gelen degisiklikler de ayni
                # yerden geciyor ve durum tek kaynaktan yayinlaniyor.
                "command_topic": self.dnd_konu,
                "payload_on": "on", "payload_off": "off",
                "icon": "mdi:bell-off-outline"}),
            ("sensor", "maliyet", {
                "name": "Gunluk maliyet",
                "unique_id": "atom_asistan_maliyet_gunluk",
                "state_topic": self.ATOM_KOK + "/maliyet",
                "unit_of_measurement": "USD",
                "state_class": "total_increasing",
                "icon": "mdi:cash"}),
            ("sensor", "uptime", {
                "name": "Calisma suresi",
                "unique_id": "atom_asistan_uptime",
                "state_topic": self.ATOM_KOK + "/uptime",
                "unit_of_measurement": "dk",
                "state_class": "measurement",
                "icon": "mdi:timer-outline"}),
            ("sensor", "rssi", {
                "name": "WiFi sinyali",
                "unique_id": "atom_asistan_rssi",
                "state_topic": self.ATOM_KOK + "/rssi",
                "unit_of_measurement": "dBm",
                "device_class": "signal_strength",
                "state_class": "measurement",
                "entity_category": "diagnostic"}),
            ("sensor", "heap", {
                "name": "Bos bellek",
                "unique_id": "atom_asistan_heap",
                "state_topic": self.ATOM_KOK + "/heap",
                "unit_of_measurement": "kB",
                "state_class": "measurement",
                "entity_category": "diagnostic",
                "icon": "mdi:memory"}),
            ("sensor", "reset", {
                "name": "Son yeniden baslatma sebebi",
                "unique_id": "atom_asistan_reset",
                "state_topic": self.ATOM_KOK + "/reset",
                "entity_category": "diagnostic",
                "icon": "mdi:restart-alert"}),
            ("binary_sensor", "dans", {
                "name": "Muzik algilandi",
                "unique_id": "atom_asistan_dans",
                "state_topic": self.ATOM_KOK + "/dans",
                "icon": "mdi:music-note"}),
            ("sensor", "dans_bpm", {
                "name": "Tempo",
                "unique_id": "atom_asistan_dans_bpm",
                "state_topic": self.ATOM_KOK + "/dans_bpm",
                "unit_of_measurement": "BPM",
                "state_class": "measurement",
                "icon": "mdi:metronome"}),
            ("sensor", "fw", {
                "name": "Cihaz surumu",
                "unique_id": "atom_asistan_fw",
                "state_topic": self.ATOM_KOK + "/fw",
                "entity_category": "diagnostic",
                "icon": "mdi:chip"}),
        ]
        if self.wake_konu:
            varliklar.append(("button", "uyandir", {
                "name": "Uyandir", "unique_id": "atom_asistan_uyandir",
                "command_topic": self.wake_konu, "payload_press": "wake",
                "icon": "mdi:microphone"}))
        n = 0
        for alan, slug, yuk in varliklar:
            yuk.update(ortak)
            if self._hass_yayinla(f"{self.HASS_KOK}/{alan}/atom_asistan/{slug}/config", yuk):
                n += 1
        self._hass_yayinla(musait, "online")
        LOG.info("HA discovery: %d varlik yayinlandi (Atom Asistan cihazi)", n)
        self._hass_durum_yayinla()
        self._hass_maliyet_yayinla()
        self._hass_saglik_yayinla()
        return n

    def _hass_saglik_yayinla(self):
        if not self._saglik:
            return
        s = self._saglik
        self._hass_yayinla(self.ATOM_KOK + "/uptime", str(s["uptime"] // 60))
        self._hass_yayinla(self.ATOM_KOK + "/rssi", str(s["rssi"]))
        self._hass_yayinla(self.ATOM_KOK + "/heap", str(s["heap"] // 1024))
        self._hass_yayinla(self.ATOM_KOK + "/reset", s["reset"])
        self._hass_yayinla(self.ATOM_KOK + "/fw", s["fw"] or "bilinmiyor")

    # ===================== AYARLARI SESLE DEGISTIRME =====================
    def _ayar_coz(self, ad: str, ham) -> tuple:
        """(deger, hata) doner. Dogrulamayi BURADA yapiyoruz: modele
        guvenip ham degeri yazmak, 'tempoyu 5000 yap' dedigin anda ayari
        bozardi. Aralik disi degeri kirpmiyoruz da - sessizce baska bir sey
        yapmaktansa reddedip sebebini soylemek dogru."""
        k = AYAR_KAYDI.get(ad)
        if not k:
            return None, (f"'{ad}' degistirilebilir ayarlar arasinda degil")
        t = k["tip"]
        m = str(ham).strip()
        if t == "bool":
            dogru = {"acik", "ac", "true", "evet", "1", "on", "aktif"}
            yanlis = {"kapali", "kapat", "false", "hayir", "0", "off", "pasif"}
            if isinstance(ham, bool):
                return ham, None
            if m.lower() in dogru:
                return True, None
            if m.lower() in yanlis:
                return False, None
            return None, f"'{m}' anlasilmadi; 'acik' ya da 'kapali' olmali"
        if t == "renk":
            if not m and k.get("bos_olabilir"):
                return "", None
            r = self._renk_dogrula(m, ad)
            if not r:
                return None, f"'{m}' gecerli bir renk degil; #RRGGBB bicimi gerekiyor"
            return r, None
        if t == "secim":
            if m.lower() in k["secenekler"]:
                return m.lower(), None
            return None, (f"'{m}' gecersiz; secenekler: "
                          + ", ".join(k["secenekler"]))
        if t in ("int", "ondalik"):
            try:
                v = int(float(m)) if t == "int" else float(m)
            except ValueError:
                return None, f"'{m}' sayi degil"
            if v < k["min"] or v > k["max"]:
                return None, (f"{v} araligin disinda; {k['min']} ile "
                              f"{k['max']} arasinda olmali")
            return v, None
        return m, None                                   # metin

    def _ayar_mevcut(self, ad: str):
        k = AYAR_KAYDI.get(ad) or {}
        alan = k.get("alan")
        if not alan:
            if ad == "log_level":
                return logging.getLevelName(
                    logging.getLogger().level).lower()
            return None
        if "." in alan:                                  # ornek: wake.threshold
            ust, alt = alan.split(".", 1)
            return getattr(getattr(self, ust, None), alt, None)
        return getattr(self, alan, None)

    async def _ayar_uygula(self, ad: str, deger):
        """Bellege yaz + yan etkiyi calistir. Yeniden baslatma gerekmiyor."""
        k = AYAR_KAYDI[ad]
        alan = k.get("alan")
        if alan:
            if "." in alan:
                ust, alt = alan.split(".", 1)
                nesne = getattr(self, ust, None)
                if nesne is not None:
                    setattr(nesne, alt, deger)
            else:
                setattr(self, alan, deger)

        sonra = k.get("sonra")
        if sonra == "renk":
            await self._renk_gonder()
        elif sonra == "ortam":
            if self.ortam_on:
                await self._ortam_gonder(zorla=True)
            else:
                await self._ortam_kapat()
        elif sonra == "dans":
            self._dans_varlik_denetlendi = False
            await self._dans_ayar_gonder()
        elif sonra == "log":
            logging.getLogger().setLevel(
                getattr(logging, str(deger).upper(), logging.INFO))
        elif sonra == "ses":
            # Ses oturum duzeyinde: acik oturum varsa bir sonraki uyanista
            # gecerli olur. Bunu kullaniciya soylememiz gerekiyor.
            pass

    async def _ayar_supervisor_yaz(self, ad: str, deger) -> tuple:
        """Ayari HA'nin kendi kaydina da yazar ki arayuzde gorunsun.

        Neden sart: yalnizca bellege yazsaydik iki ayri dogruluk kaynagi
        olurdu - sesle degistirdigin ayar arayuzde eski haliyle durur, bir
        dahaki RESTART'ta geri gelirdi. Bu projede tam olarak bu siniftan
        bir hata (secenegin koda hic ulasmamasi) saatler kaybettirdi."""
        token = os.environ.get("SUPERVISOR_TOKEN", "")
        if not token:
            return False, "SUPERVISOR_TOKEN yok (add-on olarak calismiyor)"
        ham = dict(self.cfg.get("_ham_secenekler") or {})
        ham[ad] = deger
        try:
            timeout = ClientTimeout(total=10)
            async with ClientSession(timeout=timeout) as sess:
                async with sess.post(
                        "http://supervisor/addons/self/options",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"options": ham}) as r:
                    metin = await r.text()
                    if r.status >= 400:
                        return False, f"Supervisor {r.status}: {metin[:200]}"
            self.cfg["_ham_secenekler"] = ham
            self.cfg[ad] = deger
            return True, ""
        except Exception as e:
            return False, str(e)

    async def ayar_degistir(self, ad: str, ham) -> dict:
        ad = str(ad or "").strip()
        deger, hata = self._ayar_coz(ad, ham)
        if hata:
            return {"ok": False, "hata": hata}
        eski = self._ayar_mevcut(ad)
        if eski == deger:
            return {"ok": True, "degisiklik_yok": True, "ad": ad, "deger": deger}

        await self._ayar_uygula(ad, deger)
        kalici, sebep = await self._ayar_supervisor_yaz(ad, deger)
        self._ayar_gecmisi.append((ad, eski))
        del self._ayar_gecmisi[:-20]

        LOG.info("AYAR (sesli): %s  %r -> %r   kalici=%s%s",
                 ad, eski, deger, kalici, "" if kalici else f" ({sebep})")
        sonuc = {"ok": True, "ad": ad, "eski": eski, "deger": deger,
                 "kalici": kalici}
        if not kalici:
            sonuc["uyari"] = ("degisiklik simdilik gecerli ama kaydedilemedi, "
                              "yeniden baslatinca eski haline doner: " + sebep)
        if AYAR_KAYDI[ad].get("sonra") == "ses":
            sonuc["not"] = "yeni ses bir sonraki uyanista gecerli olacak"
        return sonuc

    async def ayar_geri_al(self) -> dict:
        if not self._ayar_gecmisi:
            return {"ok": False, "hata": "geri alinacak bir degisiklik yok"}
        ad, eski = self._ayar_gecmisi.pop()
        await self._ayar_uygula(ad, eski)
        kalici, sebep = await self._ayar_supervisor_yaz(ad, eski)
        LOG.info("AYAR geri alindi: %s -> %r  kalici=%s", ad, eski, kalici)
        return {"ok": True, "ad": ad, "deger": eski, "kalici": kalici}

    def ayar_oku(self, ad: str = "") -> dict:
        ad = str(ad or "").strip()
        if ad:
            if ad not in AYAR_KAYDI:
                return {"ok": False,
                        "hata": f"'{ad}' degistirilebilir ayarlar arasinda degil"}
            return {"ok": True, "ad": ad, "deger": self._ayar_mevcut(ad),
                    "aciklama": AYAR_KAYDI[ad]["ad"]}
        return {"ok": True, "ayarlar": [
            {"ad": a, "aciklama": k["ad"], "deger": self._ayar_mevcut(a)}
            for a, k in AYAR_KAYDI.items()]}

    # ===================== DANS KAYNAGI (HA medya oynatici) ============
    async def _dans_ayar_gonder(self):
        """Cihaza dans ayarlarini bildirir. Her baglantida ve oynatici
        durumu degistiginde gidiyor."""
        if self.device is None:
            return
        await self.send_device_text({
            "type": "dans",
            "acik": self.dans_on,
            "teshis": self.dans_teshis,
            "kaynak": 1 if self.dans_kaynak == "medya" else 0,
            "caliyor": bool(self._dans_caliyor),
            "bpm": self.dans_bpm,
        })

    async def _dans_medya_oku(self):
        """HA'da caLAN bir oynatici var mi? (varlik_id, True/False) doner.

        Tek istekte tum durumlari cekiyoruz: oynatici basina ayri istek
        atmak, kullanici listeyi bos birakip 'hepsine bak' dediginde
        onlarca istege cikiyordu."""
        try:
            st = await self.ha._request("GET", "/api/states")
        except Exception as e:
            # _ortam_uyar kullanmiyoruz: o "Ortam modu: ..." diye basiyor ve
            # hatayi yanlis yeri isaret ediyordu. Tekrari yine bastiriyoruz.
            mesaj = f"oynatici durumlari alinamadi: {e}"
            if getattr(self, "_dans_uyari", None) != mesaj:
                self._dans_uyari = mesaj
                LOG.warning("Dans (medya): %s", mesaj)
            return None, False
        self._dans_uyari = None

        tum = [str(x.get("entity_id", "")) for x in st
               if str(x.get("entity_id", "")).startswith("media_player.")]

        # Yazilmis varlik gercekten var mi? Yoksa SESSIZ kalmiyoruz:
        # bir harf eksik yazilan entity_id ("..._albayrak" yerine "...")
        # hicbir hata uretmeden "hic muzik calmiyor" gibi gorunuyor ve
        # saatlerce yanlis yerde aranir. Bir kez, varolanlari listeleyerek.
        if self.dans_medya and not self._dans_varlik_denetlendi:
            self._dans_varlik_denetlendi = True
            yok = [e for e in self.dans_medya if e not in tum]
            if yok:
                LOG.warning("Dans (medya): su varlik(lar) HA'da YOK: %s. "
                            "Mevcut oynaticilar: %s",
                            ", ".join(yok), ", ".join(tum) or "(hic yok)")

        for x in st:
            eid = str(x.get("entity_id", ""))
            if not eid.startswith("media_player."):
                continue
            if self.dans_medya and eid not in self.dans_medya:
                continue
            if str(x.get("state", "")).lower() == "playing":
                return eid, True
        return None, False

    async def dans_watchdog(self):
        """Oynatici durumunu izler. Degisince cihaza haber verir."""
        while True:
            try:
                if self.dans_on and self.dans_kaynak == "medya":
                    ad, caliyor = await self._dans_medya_oku()
                    if caliyor != self._dans_caliyor:
                        self._dans_caliyor = caliyor
                        self._dans_kaynak_ad = ad
                        LOG.info("Muzik %s%s - cihaza bildirildi",
                                 "basladi" if caliyor else "durdu",
                                 f" ({ad})" if ad else "")
                        await self._dans_ayar_gonder()
            except Exception as e:
                LOG.debug("Dans izleyici: %s", e)
            await asyncio.sleep(5)

    def _hass_dans_yayinla(self):
        d = getattr(self, "_dans", None) or {"aktif": False, "bpm": 0}
        self._hass_yayinla(self.ATOM_KOK + "/dans",
                           "ON" if d["aktif"] else "OFF")
        self._hass_yayinla(self.ATOM_KOK + "/dans_bpm", str(d["bpm"] or 0))

    def _hass_durum_yayinla(self, durum: Optional[str] = None):
        """Cihazin son durumu + baglantisi + DND anahtari."""
        if durum:
            self._hass_son_durum = durum
        self._hass_yayinla(self.ATOM_KOK + "/durum",
                           self._hass_son_durum or "bilinmiyor")
        self._hass_yayinla(self.ATOM_KOK + "/baglanti",
                           "ON" if self.device is not None else "OFF")
        self._hass_yayinla(self.ATOM_KOK + "/dnd",
                           "on" if self.dnd_aktif() else "off")

    def _maliyet_gun_kontrol(self):
        """Gun donunce gunluk toplami sifirlar. Kalici: kopru yeniden
        baslayinca gunun toplami kaybolmasin."""
        bugun = time.strftime("%Y-%m-%d")
        if self._maliyet_gun == bugun:
            return
        self._maliyet_gun = bugun
        self._maliyet_gunluk = 0.0
        self._maliyet_kaydet()

    def _maliyet_yukle(self):
        try:
            with open(self._maliyet_yol, "r", encoding="utf-8") as f:
                d = json.load(f)
            self._maliyet_gun = str(d.get("gun") or "")
            self._maliyet_gunluk = float(d.get("gunluk") or 0.0)
        except FileNotFoundError:
            pass
        except Exception as e:
            LOG.warning("Gunluk maliyet okunamadi: %s", e)
        self._maliyet_gun_kontrol()

    def _maliyet_kaydet(self):
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            tmp = self._maliyet_yol + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"gun": self._maliyet_gun,
                           "gunluk": round(self._maliyet_gunluk, 6)}, f)
            os.replace(tmp, self._maliyet_yol)
        except Exception as e:
            LOG.warning("Gunluk maliyet yazilamadi: %s", e)

    def maliyet_ekle(self, usd: float):
        """Gunluk toplama ekler ve HA sensorunu gunceller."""
        self._maliyet_gun_kontrol()
        self._maliyet_gunluk += max(0.0, float(usd))
        self._maliyet_kaydet()
        self._hass_maliyet_yayinla()

    def _hass_maliyet_yayinla(self):
        self._maliyet_gun_kontrol()
        self._hass_yayinla(self.ATOM_KOK + "/maliyet",
                           f"{self._maliyet_gunluk:.4f}")

    def _dnd_durum_yayinla(self):
        """HA'nin gorebilmesi icin durumu retained olarak yayinlar."""
        if not self._mqtt:
            return
        try:
            self._mqtt.publish(
                self.dnd_konu + "/state",
                json.dumps({"acik": self.dnd_aktif(),
                            "kalan_sn": self.dnd_kalan()}),
                qos=1, retain=True)
            self._hass_yayinla(self.ATOM_KOK + "/dnd",
                               "on" if self.dnd_aktif() else "off")
        except Exception as e:
            LOG.warning("DND durumu yayinlanamadi: %s", e)

    def _threshold(self) -> float:
        return max(self._noise * self.vad_mult + 200, 500)

    def _track_noise(self, pcm: bytes) -> float:
        """Gurultu tabanini gunceller ve bu parcanin seviyesini dondurur.

        Oturum acik olsun olmasin HER pakette calisir. Eski surumde bu is
        _vad_step icindeydi; oturum kapaliyken taban guncellenmiyor, oturum
        acilinca da _in_speech True kaldigi icin yukari hic tirmanamiyordu.
        Sonuc: taban 400'de donuyor, esik 1000'de kaliyor, oda gurultusu
        1100 olunca cihaz surekli "konusuluyor" sanip hic yanit vermiyordu.
        """
        x = np.frombuffer(pcm, dtype=np.int16)
        if not x.size:
            return 0.0
        lvl = float(np.abs(x.astype(np.int32)).mean())
        self._lvl_hist.append(lvl)
        self._noise_n += 1
        # Taban = son ~5 saniyenin 20. yuzdelik dilimi. Yuzdelik kullanmak
        # ortalamadan saglam: arada konusma olsa bile taban tirmanmiyor,
        # ortam gercekten gurultulenirse birkac saniyede yukari uyum sagliyor.
        if len(self._lvl_hist) >= 25 and self._noise_n >= 10:
            self._noise_n = 0
            self._noise = float(np.percentile(np.array(self._lvl_hist), 20))
        return lvl

    def _log_level(self, lvl: float):
        """Her ~3 saniyede bir mikrofon seviyesini loglar (teshis)."""
        self._lvl_sum += lvl
        self._lvl_n += 1
        if self._lvl_n >= 150:
            LOG.info("Mikrofon seviyesi: %.0f  (gurultu tabani %.0f, esik %.0f)",
                     self._lvl_sum / self._lvl_n, self._noise, self._threshold())
            self._lvl_sum = 0.0
            self._lvl_n = 0

    async def _vad_step(self, pcm: bytes, lvl: float):
        """20 ms'lik parcaya bakarak konusma basladi/bitti kararini verir."""
        ms = 1000 * (len(pcm) // 2) // DEVICE_RATE
        threshold = self._threshold()

        if lvl > threshold:
            self._speech_ms += ms
            self._silence_ms = 0
            self._turn_speech_ms += ms
            if not self._in_speech and self._speech_ms >= self.vad_min_speech_ms:
                self._in_speech = True
                # Sayaci IPTAL etme, ILERI at. Iptal edersek ve bu tur
                # (kisa gurultu, gonderilemeyen ses vb.) hic yanit
                # uretmezse sayac bir daha kurulmuyor ve cihaz sonsuza
                # kadar "listening" ekraninda kaliyordu.
                self._sleep_at = time.time() + max(self.follow_up_window, 8.0)
                LOG.info("Konusma basladi (seviye %.0f, esik %.0f)", lvl, threshold)
                await self.set_state("listening")
                # Konusmanin ilk hecesi kaybolmasin diye son ~300 ms'i de yolla.
                # (Bu paket de tamponda oldugu icin asagida tekrar gonderilmiyor.)
                await self._flush_prebuffer(15)
                return
        else:
            self._speech_ms = max(0, self._speech_ms - ms)
            if self._in_speech:
                self._silence_ms += ms

        # ONEMLI: modele SADECE konusma sirasinda ses gonderiyoruz. Sessizlik
        # gonderilirse Whisper bos gurultuyu "ご視聴ありがとうございました" gibi
        # metinlere ceviriyor ve asistan bunlara cevap veriyordu.
        if self._in_speech:
            await self._send_audio(pcm)
            self._turn_ms += ms
            # Emniyet freni: ortam gurultusu esigin uzerine cikarsa sessizlik
            # hic olusmaz ve tur sonsuza kadar acik kalirdi. Belli sureden
            # sonra zorla kapatiyoruz.
            too_long = self._turn_ms >= self.max_turn_ms
            if self._silence_ms >= self.vad_silence_ms or too_long:
                if too_long:
                    LOG.warning("Tur %d ms surdu, zorla kapatiliyor "
                                "(ortam gurultusu esigin uzerinde olabilir)",
                                self._turn_ms)
                self._in_speech = False
                self._speech_ms = 0
                self._silence_ms = 0
                self._turn_ms = 0
                await self.commit_turn()

    async def _send_audio(self, pcm: bytes):
        out = self.up(pcm)
        if not out:
            return          # soxr akisi cikti biriktiriyor; bos paket gonderme
        try:
            await self.oai.send(json.dumps({
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(out).decode(),
            }))
            self._turn_open = True
        except Exception as e:
            LOG.warning("OpenAI'ye ses gonderilemedi: %s", e)

    async def commit_turn(self):
        """Biriken sesi kapatip modelden yanit ister."""
        if self.oai is None:
            return
        if not self._turn_open:
            # Ses hic gonderilemedi (bos resample ciktisi, kopuk baglanti).
            # Uyku sayacini yeniden kur, yoksa burada sessizce kayboluyordu.
            self._arm_sleep()
            return
        self._turn_open = False
        spoken = self._turn_speech_ms
        self._turn_speech_ms = 0

        # Cok kisa gurultu: modele hic gonderme, tamponu bosalt.
        if spoken < self.min_utterance_ms:
            LOG.info("Cok kisa ses (%d ms), gurultu sayildi - atiliyor", spoken)
            try:
                await self.oai.send(json.dumps({"type": "input_audio_buffer.clear"}))
            except Exception:
                pass
            self._arm_sleep()
            return

        LOG.info("Konusma bitti (%d ms), yanit isteniyor", spoken)
        self.last_activity = time.time()
        await self.set_state("thinking")
        try:
            await self.oai.send(json.dumps({"type": "input_audio_buffer.commit"}))
            if not self._response_active:
                await self.oai.send(json.dumps({"type": "response.create"}))
        except Exception as e:
            LOG.warning("Turn kapatilamadi: %s", e)

    async def on_device_audio(self, pcm: bytes):
        # Gurultu tabani her zaman guncellenmeli - uyurken de.
        lvl = self._track_noise(pcm)
        self._log_level(lvl)
        self._prebuf.append(pcm)          # her zaman doluyor, geriye donuk kayit

        # Oturum kuruluyorsa sesi sadece tampona al ve DON. start_turn'u
        # burada await edersek "async for msg in ws" dongusu duruyor,
        # cihaz 20 ms'de bir paket yolladigi icin alim kuyrugu doluyor ve
        # cihaz baglantiyi dusuruyordu. Eski surumde start_turn ~800 ms
        # suruyordu ve sinirin altinda kaliyordu; prebuffer eklenince
        # ~2 sn'ye cikti ve sinir asildi.
        if self._turn_starting:
            return

        if self.oai is None or self._dinleme_kapali:
            # Rahatsiz etme: wake word'u hic degerlendirme. Toplanti sirasinda
            # yanlis tetikleme olmasin diye tam burada kesiyoruz - oturum
            # acilmadigi icin token da harcanmiyor.
            if self.dnd_aktif():
                return
            if self.wake.enabled and self.wake.feed(pcm):
                LOG.info("Wake word yakalandi (seviye %.0f, esik %.0f)",
                         lvl, self._threshold())
                self._turn_starting = True
                asyncio.create_task(self._start_turn_bg("wake word"))
            return
        if self.speaking or self._response_active:
            return
        await self._vad_step(pcm, lvl)

    # ------------------------------------------------------ OpenAI tarafi
    # ------------------------------------------------------ kisisel hafiza
    def _hafiza_yukle(self) -> list:
        try:
            with open(self.hafiza_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            return [str(x) for x in (d.get("notlar") or [])]
        except FileNotFoundError:
            return []
        except Exception as e:
            LOG.warning("Hafiza okunamadi (%s), bos baslatiliyor: %s",
                        self.hafiza_path, e)
            return []

    def _hafiza_kaydet(self):
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            tmp = self.hafiza_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"notlar": self.hafiza_ogrenilen}, f,
                          ensure_ascii=False, indent=1)
            os.replace(tmp, self.hafiza_path)      # yarim dosya kalmasin
        except Exception as e:
            LOG.warning("Hafiza yazilamadi: %s", e)

    def hafiza_ekle(self, bilgi: str) -> dict:
        bilgi = " ".join(str(bilgi).split()).strip()
        if not bilgi:
            return {"error": "bos bilgi"}
        if len(bilgi) > 300:
            bilgi = bilgi[:297] + "..."
        dusuk = bilgi.lower()
        for v in self.hafiza_sabit + self.hafiza_ogrenilen:
            if v.lower() == dusuk:
                return {"ok": True, "zaten_vardi": True}
        self.hafiza_ogrenilen.append(bilgi)
        # Sinirsiz buyumesin; en eskiyi at.
        if len(self.hafiza_ogrenilen) > 80:
            self.hafiza_ogrenilen = self.hafiza_ogrenilen[-80:]
        self._hafiza_kaydet()
        LOG.info("Hafizaya eklendi: %s", bilgi)
        return {"ok": True, "toplam": len(self.hafiza_ogrenilen)}

    def hafiza_sil(self, arama: str) -> dict:
        arama = (arama or "").strip().lower()
        if not arama:
            return {"error": "bos arama"}
        kalan = [x for x in self.hafiza_ogrenilen if arama not in x.lower()]
        silinen = len(self.hafiza_ogrenilen) - len(kalan)
        self.hafiza_ogrenilen = kalan
        if silinen:
            self._hafiza_kaydet()
            LOG.info("Hafizadan %d satir silindi (arama: %s)", silinen, arama)
        # Ayarlardaki sabit satirlar buradan silinemez - kullaniciya soyle.
        sabit_esles = [x for x in self.hafiza_sabit if arama in x.lower()]
        return {"ok": True, "silinen": silinen,
                "ayarlardan_silinmesi_gereken": sabit_esles}

    def hafiza_listesi(self) -> list:
        return self.hafiza_sabit + self.hafiza_ogrenilen

    # -------------------------------------------------------- zamanlayici
    def _timer_yukle(self):
        try:
            with open(self.timers_path, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            return
        now = time.time()
        for t in d.get("timers") or []:
            try:
                at = float(t["at"])
            except Exception:
                continue
            if at <= now:
                continue          # kopru kapaliyken gecmis, sessizce dus
            self._timer_seq += 1
            self.timers[self._timer_seq] = {
                "at": at,
                "etiket": str(t.get("etiket") or ""),
                "total": int(t.get("total") or max(1, int(at - now))),
            }
        if self.timers:
            LOG.info("%d zamanlayici geri yuklendi", len(self.timers))

    def _timer_kaydet(self):
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            tmp = self.timers_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"timers": list(self.timers.values())}, f)
            os.replace(tmp, self.timers_path)
        except Exception as e:
            LOG.warning("Zamanlayici yazilamadi: %s", e)

    def timer_kur(self, saniye: int, etiket: str = "") -> dict:
        try:
            saniye = int(saniye)
        except Exception:
            return {"error": "saniye sayi olmali"}
        if saniye < 1 or saniye > 86400:
            return {"error": "sure 1 saniye ile 24 saat arasinda olmali"}
        self._timer_seq += 1
        tid = self._timer_seq
        self.timers[tid] = {"at": time.time() + saniye,
                            "etiket": " ".join(str(etiket or "").split())[:60],
                            "total": saniye}
        self._timer_kaydet()
        LOG.info("Zamanlayici #%d kuruldu: %d sn (%s)", tid, saniye,
                 self.timers[tid]["etiket"] or "etiketsiz")
        return {"ok": True, "id": tid, "saniye": saniye,
                "etiket": self.timers[tid]["etiket"]}

    def timer_liste(self) -> dict:
        now = time.time()
        return {"zamanlayicilar": [
            {"id": i, "kalan_saniye": max(0, int(round(t["at"] - now))),
             "etiket": t["etiket"]}
            for i, t in sorted(self.timers.items(), key=lambda kv: kv[1]["at"])
        ]}

    def timer_iptal(self, tid=None) -> dict:
        if tid is None:
            if len(self.timers) == 1:
                tid = next(iter(self.timers))
            elif not self.timers:
                return {"error": "kurulu zamanlayici yok"}
            else:
                return {"error": "birden fazla zamanlayici var, id gerekli",
                        **self.timer_liste()}
        try:
            tid = int(tid)
        except Exception:
            return {"error": "id sayi olmali"}
        t = self.timers.pop(tid, None)
        if t is None:
            return {"error": f"#{tid} bulunamadi"}
        self._timer_kaydet()
        LOG.info("Zamanlayici #%d iptal edildi", tid)
        return {"ok": True, "iptal_edilen": tid, "etiket": t["etiket"]}

    def _timer_yakin(self):
        """Ekranda gosterilecek en yakin zamanlayici."""
        if not self.timers:
            return None
        tid = min(self.timers, key=lambda i: self.timers[i]["at"])
        return tid, self.timers[tid]

    async def _timer_ekrani_guncelle(self, zorla: bool = False):
        """Cihaza kalan sureyi bildirir. Cihaz kendi geri sayar; biz sadece
        kurulumda, iptalde ve arada bir senkron icin gonderiyoruz."""
        y = self._timer_yakin()
        if y is None:
            if self._timer_pushed != 0 or zorla:
                self._timer_pushed = 0
                await self.send_device_text({"type": "timer", "left": 0})
            return
        _, t = y
        left = max(0, int(round(t["at"] - time.time())))
        if zorla or abs(left - self._timer_pushed) >= 10 or self._timer_pushed <= 0:
            self._timer_pushed = left
            await self.send_device_text({"type": "timer", "left": left,
                                         "total": int(t["total"])})

    # ===================== ORTAM MODU (saat + hava) =====================
    def _ortam_uyar(self, anahtar: str, mesaj: str):
        """Ayni ortam hatasini her turda tekrar basma - ama sustuma da.
        Hata degisirse ya da duzelip tekrar bozulursa yeniden yazar."""
        if self._ortam_uyari.get(anahtar) == mesaj:
            return
        self._ortam_uyari[anahtar] = mesaj
        LOG.warning("Ortam modu: %s", mesaj)

    async def _ortam_oku(self):
        """HA'dan hava durumu ve sicakligi okur. (hava_kodu, sicaklik) doner;
        okunamayan alan None olur - o alan ekranda gosterilmez."""
        hava, isi = None, None
        if not self.ortam_hava_goster:
            return None, None        # sadece saat istendi; HA'ya hic gitme
        # Ayar bos ise HA'daki ilk weather varligini kendimiz buluyoruz.
        # Sebep: add-on guncellenirken HA yeni secenegin varsayilanini
        # mevcut kuruluma tasimiyor; kullanici Configuration sekmesine
        # girip elle yazana kadar ekranda hava hic cikmiyordu. Sectigimiz
        # varligi loga yaziyoruz - yanlissa ortam_hava_varlik ile ezilir.
        if not self.ortam_hava_varlik and not self._ortam_arandi:
            self._ortam_arandi = True
            try:
                st = await self.ha._request("GET", "/api/states")
                bulunan = [x.get("entity_id", "") for x in st
                           if str(x.get("entity_id", "")).startswith("weather.")]
                if bulunan:
                    self.ortam_hava_varlik = bulunan[0]
                    LOG.warning("Ortam modu: hava varligi ayarlanmamis, "
                                "otomatik secildi -> %s%s  (baskasini istiyorsan "
                                "ortam_hava_varlik ayarini doldur)",
                                bulunan[0],
                                f"  [digerleri: {', '.join(bulunan[1:])}]"
                                if len(bulunan) > 1 else "")
                else:
                    LOG.warning("Ortam modu: HA'da hic weather varligi yok - "
                                "ekranda yalniz saat gorunecek")
            except Exception as e:
                self._ortam_arandi = False       # HA yoksa sonra tekrar dene
                self._ortam_uyar("arama", f"varlik listesi alinamadi: {e}")
        if self.ortam_hava_varlik:
            try:
                st = await self.ha._request(
                    "GET", f"/api/states/{self.ortam_hava_varlik}")
                durum = str(st.get("state", "")).lower()
                hava = HAVA_ESLEME.get(durum)
                if hava is None and durum not in ("unknown", "unavailable", ""):
                    # Bilmedigimiz bir durum: sessizce yutmak yerine loga yaz,
                    # HAVA_ESLEME'ye eklenmesi gerekiyor demektir.
                    LOG.info("Bilinmeyen hava durumu '%s' - ikon gosterilmiyor",
                             durum)
                oz = st.get("attributes") or {}
                if oz.get("temperature") is not None:
                    isi = float(oz["temperature"])
                self._ortam_uyari.pop("hava", None)   # duzeldi, hafizayi temizle
            except Exception as e:
                # DEBUG DEGIL, WARNING. Ilk surumde debug'daydi ve "hava=yok"
                # yaziyordu ama NEDENI loga hic dusmuyordu; varligin adi mi
                # yanlis, HA mi cevap vermiyor ayirt edilemiyordu.
                # Tekrari bastiriyoruz: 2 dakikada bir ayni satiri basmasin.
                self._ortam_uyar("hava", f"'{self.ortam_hava_varlik}' okunamadi: {e}")
        if self.ortam_isi_varlik:
            try:
                st = await self.ha._request(
                    "GET", f"/api/states/{self.ortam_isi_varlik}")
                isi = float(st.get("state"))
                self._ortam_uyari.pop("isi", None)
            except Exception as e:
                self._ortam_uyar("isi", f"'{self.ortam_isi_varlik}' okunamadi: {e}")
        return hava, isi

    async def _ortam_gonder(self, zorla: bool = False):
        """Cihaza saat + hava + sicaklik yollar. Degisen bir sey yoksa
        gondermez: cihaz dakikalari kendi sayiyor, ayni mesaji tekrar
        yollamak bosuna trafik."""
        if not self.ortam_on or self.device is None:
            return
        simdi = time.localtime()
        dk = simdi.tm_hour * 60 + simdi.tm_min
        hava, isi = await self._ortam_oku()
        c = None if isi is None else int(round(isi))
        yeni = (dk, hava, c)
        if not zorla and self._ortam_son == yeni:
            return
        self._ortam_son = yeni
        await self.send_device_text({"type": "ambient", "dk": dk,
                                     "hava": HV_YOK if hava is None else hava,
                                     "c": -1000 if c is None else c})
        # Loga yaz: bu satir yoksa "saat ekrani neden gelmedi" sorusunu
        # cevaplamanin tek yolu cihazin seri portuna bakmak oluyordu.
        LOG.info("Ortam gonderildi: %02d:%02d  hava=%s  sicaklik=%s",
                 dk // 60, dk % 60,
                 "yok" if hava is None else hava,
                 "yok" if c is None else f"{c}C")

    # ===================== YUZ RENKLERI ================================
    @staticmethod
    def _renk_dogrula(ham, ad: str) -> str:
        """'#5EE8E0' / '5ee8e0' / '0x5EE8E0' -> '#5EE8E0'. Bozuksa bos.
        Sessizce yutmuyoruz: yanlis yazilan bir renk kodu ekranda hicbir
        sey degistirmiyor ve kullanici sebebini bilemiyor."""
        m = str(ham or "").strip().lstrip("#").strip()
        if not m:
            return ""
        if m[:2].lower() == "0x":
            m = m[2:]
        if len(m) == 3:        # #abc -> #aabbcc
            m = "".join(c * 2 for c in m)
        if len(m) != 6 or any(c not in "0123456789abcdefABCDEF" for c in m):
            LOG.warning("Yuz rengi (%s) anlasilmadi: %r - varsayilan kullanilacak. "
                        "Bicim: #RRGGBB, ornek #5EE8E0", ad, ham)
            return ""
        return "#" + m.upper()

    async def _renk_gonder(self):
        """Cihaza renkleri bildirir. Her baglantida yollaniyor; cihaz ayni
        degeri tekrar aldiginda flash'a yazmiyor."""
        if self.device is None:
            return
        await self.send_device_text({"type": "renk",
                                     "goz": self.goz_renk,
                                     "agiz": self.agiz_renk})
        LOG.info("Yuz renkleri gonderildi: goz=%s  agiz=%s",
                 self.goz_renk or "(varsayilan)",
                 self.agiz_renk or "(goz ile ayni)")

    async def _ortam_kapat(self):
        """Cihaza 'saat ekranini unut' der. Ayar kapatildiginda cagriliyor."""
        if self.device is None:
            return
        self._ortam_son = None
        await self.send_device_text({"type": "ambient", "dk": -1,
                                     "hava": HV_YOK, "c": -1000})
        LOG.info("Ortam modu kapali - cihaza saat ekrani temizletildi")

    async def ortam_watchdog(self):
        """Saati ve havayi periyodik tazeler. Cihaz uykuda olmasa da
        gonderiyoruz - uykuya girdigi anda ekran hazir olsun."""
        while True:
            try:
                await self._ortam_gonder()
            except Exception as e:
                LOG.debug("Ortam guncellemesi basarisiz: %s", e)
            await asyncio.sleep(self.ortam_periyot)

    async def timer_watchdog(self):
        """Suresi dolani calistirir, ekrani senkron tutar."""
        while True:
            await asyncio.sleep(1.0)
            try:
                now = time.time()
                dolan = [i for i, t in self.timers.items() if t["at"] <= now]
                for i in dolan:
                    t = self.timers.pop(i)
                    self._timer_kaydet()
                    etiket = t["etiket"]
                    LOG.info("Zamanlayici #%d doldu (%s)", i, etiket or "etiketsiz")
                    dk = max(1, int(round(t["total"] / 60)))
                    if etiket:
                        talimat = (f"Kurdugun zamanlayici doldu. Konu: {etiket}. "
                                   f"Sure: {dk} dakika. Kullaniciya bunu kisa ve "
                                   f"dogal bir cumleyle hatirlat.")
                    else:
                        talimat = (f"{dk} dakikalik zamanlayici doldu. Kullaniciya "
                                   f"kisa bir cumleyle haber ver.")
                    await self.proaktif_konus(talimat, kaynak=f"zamanlayici#{i}")
                await self._timer_ekrani_guncelle()
            except Exception as e:
                LOG.warning("timer_watchdog hatasi: %s", e)

    # --------------------------------------------------- proaktif konusma
    async def proaktif_konus(self, talimat: str, kaynak: str = "bildirim"):
        """Kullanici sormadan asistani konusturur (zamanlayici, HA bildirimi).
        Oturum kapaliysa acar; konusma bittikten sonra normal takip suresi
        isler, yani kullanici hemen cevap verebilir."""
        talimat = (talimat or "").strip()
        if not talimat:
            return {"error": "bos talimat"}
        if self.device is None:
            LOG.warning("Proaktif konusma atlandi (%s): cihaz bagli degil", kaynak)
            return {"error": "cihaz bagli degil"}
        # Rahatsiz etme: konusma. Zamanlayici gibi seyleri kaybetmeyelim diye
        # kuyruga alip DND bitince tek ozetle iletiyoruz.
        if self.dnd_aktif() and kaynak != "dnd-kuyruk":
            if self.dnd_kuyruk_max > 0:
                if len(self.dnd_kuyruk) < self.dnd_kuyruk_max:
                    self.dnd_kuyruk.append(talimat)
                    LOG.info("Rahatsiz etme acik - bildirim kuyruga alindi "
                             "(%s, kuyrukta %d)", kaynak, len(self.dnd_kuyruk))
                else:
                    LOG.info("Rahatsiz etme acik - kuyruk dolu, bildirim "
                             "atlandi (%s)", kaynak)
            else:
                LOG.info("Rahatsiz etme acik - bildirim atlandi (%s)", kaynak)
            return {"ok": True, "ertelendi": True}
        LOG.info("Proaktif konusma (%s): %s", kaynak, talimat[:120])
        try:
            await self.ensure_openai()
        except Exception as e:
            LOG.warning("Proaktif konusma icin oturum acilamadi: %s", e)
            return {"error": str(e)}
        if self.oai is None:
            return {"error": "oturum yok"}
        # Bekleyen bir yanit varsa arasina girme, sirasini beklesin.
        for _ in range(60):
            if not self._response_active:
                break
            await asyncio.sleep(0.25)
        self.last_activity = time.time()
        self._reset_vad()
        try:
            await self.oai.send(json.dumps({
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "system",
                    "content": [{"type": "input_text", "text":
                                 "SISTEM BILDIRIMI (kullanici bir sey sormadi, "
                                 "sen konusmayi baslatiyorsun). Bu bildirimi "
                                 "kullaniciya SESLI olarak, tek kisa cumleyle "
                                 "ilet. Arac cagirma, soru sorma: " + talimat}],
                },
            }))
            # tool_choice="none" burada sart. Aksi halde model bildirimi
            # konusmak yerine bir arac cagirip susabiliyor - zamanlayici
            # doldugunda tam olarak bu oldu: set_face cagrildi, ses cikmadi.
            await self.oai.send(json.dumps({
                "type": "response.create",
                "response": {
                    "output_modalities": ["audio"],
                    "tool_choice": "none",
                },
            }))
            self._proaktif_bekleyen = kaynak
        except Exception as e:
            LOG.warning("Proaktif konusma gonderilemedi: %s", e)
            return {"error": str(e)}
        await self.set_state("speaking")
        self._arm_sleep()
        return {"ok": True}

    # ------------------------------------------------- HA bildirimleri
    def _mqtt_mesaj(self, konu: str, ham: str):
        """Tek bir MQTT mesajini isler. paho thread'inde calisir; asyncio
        islerini run_coroutine_threadsafe ile donguye atar. Istisna
        firlatabilir - cagiran on_message hepsini yakaliyor."""
        if self.wake_konu and konu == self.wake_konu:
            # Yuk onemsiz: mesajin gelmesi "Hey Jarvis" demekle ayni.
            LOG.info("Uzaktan uyandirma istegi (%s)", self.wake_konu)
            if self._loop:
                asyncio.run_coroutine_threadsafe(
                    self.uzaktan_uyandir("kisayol"), self._loop)
            return

        # Rahatsiz etme konusu: "on"/"off"/"30" ya da {"acik":true,"dakika":30}
        if konu == self.dnd_konu:
            acik, dakika = None, None
            d = ham.lower()
            if d.startswith("{"):
                try:
                    j = json.loads(ham)
                    acik = bool(j.get("acik", j.get("on", True)))
                    dakika = j.get("dakika", j.get("minutes"))
                except json.JSONDecodeError:
                    LOG.warning("DND mesaji bozuk JSON: %s", ham[:80])
                    return
            elif d in ("on", "ac", "aç", "true", "1", "acik", "açık"):
                acik = True
            elif d in ("off", "kapat", "false", "0", "kapali", "kapalı"):
                acik = False
            elif d in ("toggle", "cevir", "çevir", "degistir", "değiştir"):
                # Klavye kisayolu / tek dugme icin: gonderen tarafin
                # mevcut durumu bilmesi gerekmiyor.
                acik = not self.dnd_aktif()
            else:
                try:
                    dakika, acik = int(float(d)), True
                except ValueError:
                    LOG.warning("DND mesaji anlasilmadi: %s", ham[:80])
                    return
            if self._loop:
                asyncio.run_coroutine_threadsafe(
                    self.dnd_ayarla(acik, dakika, kaynak="HA"), self._loop)
            return

        talimat, kaynak = ham, "HA"
        if ham.startswith("{"):
            try:
                d = json.loads(ham)
                talimat = str(d.get("prompt") or d.get("text") or "").strip()
                kaynak = str(d.get("kaynak") or d.get("source") or "HA")
            except json.JSONDecodeError:
                pass
        if not talimat:
            LOG.warning("MQTT bildirimi bos, atlandi")
            return
        if self._loop is None:
            LOG.warning("Bildirim geldi ama dongu hazir degil, atlandi")
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self.proaktif_konus(talimat, kaynak=kaynak), self._loop)
        except Exception as e:
            LOG.warning("Bildirim isleme aktarilamadi: %s", e)

    def mqtt_baslat(self):
        """HA otomasyonlarindan gelen konusma isteklerini dinler.
        paho senkron calisir; kendi thread'inde donup coroutine'leri
        asyncio dongusune geri atiyoruz."""
        if not self.notify_on:
            return
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            LOG.error("notify_enabled acik ama paho-mqtt kurulu degil. "
                      "Add-on'u Rebuild et.")
            return

        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                 client_id="atom-asistan-bridge")
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id="atom-asistan-bridge")  # paho 1.x

        if self.mqtt_user:
            client.username_pw_set(self.mqtt_user, self.mqtt_pass)

        topic = self.notify_topic

        def on_connect(c, userdata, flags, rc, *args):
            # paho 1.x -> rc int;  2.x -> ReasonCode nesnesi.
            # ReasonCode int() kabul etmiyor, .value ile sayiya cevriliyor.
            kod = getattr(rc, "value", rc)
            try:
                kod = int(kod)
            except (TypeError, ValueError):
                kod = -1
            if kod == 0:
                c.subscribe(topic, qos=1)
                c.subscribe(self.dnd_konu, qos=1)
                if self.wake_konu:
                    c.subscribe(self.wake_konu, qos=1)
                LOG.info("MQTT baglandi. Bildirim: %s   Rahatsiz etme: %s   "
                         "Uyandirma: %s", topic, self.dnd_konu,
                         self.wake_konu or "kapali")
                self._dnd_durum_yayinla()
                self._hass_discovery()
            else:
                LOG.error("MQTT baglanamadi (rc=%s / %s). Kullanici ve sifre "
                          "dogru mu? Mosquitto add-on'u HA kullanicilarini "
                          "kabul ediyor.", kod, rc)

        def on_message(c, userdata, msg):
            # Buradaki HER istisna paho thread'ini oldurur: MQTT sessizce
            # durur ve kopru yeniden baslayana kadar geri gelmez. Eskiden
            # bu yorum vardi ama try yalnizca ilk satiri sariyordu; DND
            # kisayolundaki bir hata tam da bu yuzden MQTT'yi oldurdu.
            # Artik govdenin TAMAMI korumali; is mantigi _mqtt_mesaj'da.
            try:
                ham = msg.payload.decode("utf-8", "replace").strip()
                if ham:
                    self._mqtt_mesaj(msg.topic, ham)
            except Exception:
                LOG.exception("MQTT mesaji islenemedi (%s) - baglanti "
                              "korunuyor", getattr(msg, "topic", "?"))

        def on_disconnect(*a):
            LOG.warning("MQTT baglantisi koptu, yeniden denenecek")

        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        try:
            client.connect_async(self.mqtt_host, self.mqtt_port, keepalive=60)
            client.loop_start()
            self._mqtt = client
            LOG.info("MQTT baglantisi baslatildi: %s:%d", self.mqtt_host, self.mqtt_port)
        except Exception as e:
            LOG.error("MQTT baslatilamadi: %s", e)

    def system_prompt(self) -> str:
        base = self.cfg.get("instructions", "").strip()
        base += f"\n\nCihazin su anki hoparlor seviyesi: {self.volume}/100."
        base += (
            "\n\nDINLEME KURALLARI - bunlar teknik gercek, kullaniciya yanlis bilgi verme:"
            "\n- Cihaz normalde UYKUDA. Sadece wake word duyunca uyanir."
            f"\n- Her yanitindan sonra {self.follow_up_window:.0f} saniye daha dinler; "
            "kimse konusmazsa kendiliginden uykuya doner. Yani 'surekli dinliyorum' degil."
            "\n- Dinlemeyi istedigin an birakabilirsin: sleep aracini cagir. "
            "'Dinlemeyi birak', 'uyu', 'tamam bu kadar', 'tesekkurler' gibi konusmayi "
            "bitiren her seyde once kisa bir veda et, sonra sleep'i cagir."
            "\n- Mikrofonu kapatamayacagini SOYLEME; sleep aracin var."
            "\n- Duydugun sey anlamsizsa, baska bir dilde sacma bir metne benziyorsa ya da "
            "sana soylenmis bir komut degilse HIC YANIT VERME: sadece sleep'i cagir. "
            "Arka plan gurultusune 'bir sey duymadim' diye cevap verme, sessiz kal."
        )
        if self.hypno_saniye > 0:
            # Yuzu kopru degistiriyor (transkriptten yakaliyor), modelin
            # yapacagi bir sey yok. Ama haberi olmazsa "anlamadim" diyor
            # ve sakanin tadi kaciyor.
            base += (
                "\n\nGIZLI OZELLIK - HIPNOZ MODU: Kullanici 'hypno toad "
                "moduna gec', 'kurbaga moduna gec', 'hipnoz modu' ya da "
                "bunlara benzeyen bir sey soylerse hipnoz_modu aracini "
                "acik=true ile CAGIR - yoksa gozler degismez, sadece "
                "konusmus olursun. Konusma metne cevrilirken bu kelimeler "
                "bozuluyor ('hipnotogruk', 'kurban modu', 'hypno toast'); "
                "anlami tutuyorsa yazim tutmasa da cagir. Aracı cagirdiktan "
                "sonra kisa ve oyuncu bir cumle soyle (ornek: 'Butun ovgu "
                "Hypnotoad'a.'). 'Normale don' dendiginde acik=false ile cagir."
            )
        if not (self.pc_show and self.pc_topic):
            base += ("\n\nNOT: Ekrana yansitma su an kapali. Kullanici ekrana "
                     "yazmani isterse yapamayacagini kisaca soyle.")
        notlar = self.hafiza_listesi()
        if notlar:
            base += ("\n\nKULLANICI HAKKINDA BILDIKLERIN - bunlari sormadan "
                     "uygula, her seferinde teyit isteme:\n- "
                     + "\n- ".join(notlar))
        if self.timers_on:
            y = self._timer_yakin()
            if y:
                _, t = y
                kalan = max(0, int(round(t["at"] - time.time())))
                base += (f"\n\nSu an kurulu bir zamanlayici var: "
                         f"{kalan // 60} dk {kalan % 60} sn kaldi"
                         + (f" ({t['etiket']})" if t["etiket"] else "") + ".")
        if self.ha.entity_catalog:
            base += ("\n\nKontrol edebilecegin Home Assistant varliklari "
                     "(entity_id = ad):\n" + self.ha.entity_catalog)
        return base

    def _reset_vad(self):
        self._in_speech = False
        self._speech_ms = 0
        self._silence_ms = 0
        self._turn_open = False
        self._turn_speech_ms = 0
        self._turn_ms = 0

    def _arm_sleep(self):
        """Yanit bitti; bu kadar sn icinde konusan olmazsa oturumu kapat."""
        self._sleep_at = time.time() + self.follow_up_window

    async def dinlemeyi_durdur(self, reason: str):
        """Mikrofonu birakir ama OTURUMU ACIK TUTAR.

        Eskiden takip suresi dolunca oturum da kapaniyordu; pencereyi
        kisaltmak bu yuzden pahaliydi - her takip sorusu yeni oturum,
        yeni sistem prompt'u, yeni varlik katalogu demekti. Artik pencere
        yalniz dinlemeyi bitiriyor.
        """
        if self.oai is None or self._dinleme_kapali:
            return
        LOG.info("Dinleme birakildi (%s) - oturum acik, wake word bekleniyor",
                 reason)
        self._dinleme_kapali = True
        self._sleep_at = None
        self._reset_vad()
        self._prebuf.clear()
        self.wake.reset()
        if self.eslik_dk > 0:
            # Uyanik ama sakin. Cihaz kendi icinde sese kisa bakislar
            # atiyor; kopru burada baska bir sey yapmiyor.
            self._eslik_at = time.time()
            await self.set_state("idle", "neutral")
            LOG.info("Eslik modu: %d dakika uyanik kalacak", self.eslik_dk)
        else:
            self._eslik_at = None
            await self.set_state("sleep", "neutral")

    async def go_to_sleep(self, reason: str):
        """Oturumu kapatir, cihaz wake word bekleyen uyku moduna doner."""
        if self.oai is None:
            self._dinleme_kapali = False
            return
        LOG.info("Uyku moduna geciliyor (%s) - wake word bekleniyor", reason)
        self._sleep_at = None
        self._eslik_at = None
        self._reset_vad()
        # DIKKAT: bayragi burada TEMIZLEME. close_openai ~2 saniye suruyor
        # ve bu sure bir await; bayrak simdi acilirsa o aralikta gelen ses
        # paketleri VAD'a girip cihaza "listening" yolluyor. Sonuc: uykuya
        # giderken yuz bir anligina dinlemeye donuyor ve ORTAM MODUNUN
        # 5 dakikalik sayaci bastan basliyor (gozlendi: 21:54:19 uyku ->
        # 21:54:20 "konusma basladi" -> 21:54:21 uyku; saat ekrani 3
        # dakika gecikti). Once kapat, sonra ac.
        self._dinleme_kapali = True
        await self.close_openai()
        self._prebuf.clear()
        self.wake.reset()
        self._dinleme_kapali = False
        # Cihazda gozler kapanip "z" belirir; bosta durusla karismasin.
        await self.set_state("sleep", "neutral")

    async def _uyandir(self, kaynak: str):
        """Gerekirse DND'yi kapatip turu baslatir.

        Ikisi ayri create_task olsaydi sira garanti olmazdi; dnd_ayarla
        "idle" yuzunu start_turn'un "listening"inden SONRA gonderip
        ekrani yanlis birakabilirdi.
        """
        if self.dnd_aktif():
            # Butona uzun basan ya da kisayola basan biri konusmak istiyor
            # demektir; sadece susmayi bitirip birakmak yarim is olurdu.
            await self.dnd_ayarla(False, kaynak=kaynak)
        if self.hypno_aktif():
            await self.hypno_ayarla(False, kaynak=kaynak)
        await self._start_turn_bg(kaynak)

    async def _buton_uyandir(self):
        await self._uyandir("buton")

    async def uzaktan_uyandir(self, kaynak: str = "kisayol"):
        """MQTT'den gelen uyandirma. Cihaz yoksa bosuna oturum acmiyoruz."""
        if self.device is None:
            LOG.warning("Uyandirma istegi (%s) geldi ama cihaz bagli degil", kaynak)
            return
        if self._turn_starting:
            LOG.info("Uyandirma istegi (%s) yok sayildi - tur zaten aciliyor",
                     kaynak)
            return
        self._turn_starting = True
        await self._uyandir(kaynak)

    async def _start_turn_bg(self, kaynak: str):
        """start_turn'u cihaz okuma dongusunun DISINDA calistirir."""
        t0 = time.time()
        try:
            await self.start_turn()
        except Exception as e:
            metin = str(e)
            if "401" in metin or "403" in metin or "invalid_api_key" in metin.lower():
                LOG.error("OpenAI oturumu ACILAMADI (%s): API ANAHTARI GECERSIZ. "
                          "Add-on ayarlarindaki openai_api_key'i kontrol et. (%s)",
                          kaynak, metin[:200])
            elif "429" in metin or "quota" in metin.lower():
                LOG.error("OpenAI oturumu acilamadi (%s): KOTA/LIMIT sorunu. (%s)",
                          kaynak, metin[:200])
            else:
                LOG.error("Tur baslatilamadi (%s): %s: %s",
                          kaynak, type(e).__name__, metin[:200])
            await self.set_state("error")
            await self.close_openai()
        finally:
            self._turn_starting = False
            LOG.info("Tur hazir (%s) - %d ms", kaynak, int((time.time() - t0) * 1000))

    async def start_turn(self):
        # DIKKAT: once tazele, sonra oturum ac. ensure_openai icinde self.oai
        # atandigi anda idle_watchdog bu turu gecerli sayiyor; last_activity
        # hala onceki oturumdan kalma eski deger olursa watchdog daha
        # session.update bitmeden "bosta kalindi" deyip oturumu olduruyordu.
        self.last_activity = time.time()
        self._dinleme_kapali = False
        await self.ensure_openai()
        self.last_activity = time.time()
        # Onceki denemeden kalan sesi at, yoksa yeni istekle birikip karisiyor.
        if self.oai is not None and self._turn_open:
            try:
                await self.oai.send(json.dumps({"type": "input_audio_buffer.clear"}))
                LOG.info("Onceki ses tamponu temizlendi")
            except Exception:
                pass
        self._reset_vad()
        self._out_carry = b""
        # Sayaci ONCE ileri at. _flush_prebuffer 250 parcayi tek tek
        # gonderdigi icin ~400 ms suruyor ve o sirada watchdog calisiyor;
        # sayac bos ya da gecmiste kalirsa oturumu daha dogmadan olduruyor.
        self._sleep_at = time.time() + max(self.follow_up_window, 8.0)
        self.up = Resampler(DEVICE_RATE, OAI_RATE)
        await self._flush_prebuffer()
        # Wake word sesi zaten tampondan gonderildi. Turu zorla "konusuluyor"
        # saymiyoruz: ortam gurultusu esigin uzerindeyse tur hic kapanmiyordu.
        # Kullanici devam ederse VAD kendisi yakalar; hic konusmazsa asagidaki
        # sayac cihazi uykuya dondurur.
        self._sleep_at = time.time() + max(self.follow_up_window, 8.0)
        await self.set_state("listening")

    async def _flush_prebuffer(self, max_chunks: Optional[int] = None):
        """Oturum acilmadan onceki son saniyeleri modele geriye donuk gonderir."""
        if self.oai is None or not self._prebuf:
            return
        chunks = list(self._prebuf)
        if max_chunks:
            chunks = chunks[-max_chunks:]
        self._prebuf.clear()
        sent = 0
        for pcm in chunks:
            out = self.up(pcm)
            if not out:
                continue
            try:
                await self.oai.send(json.dumps({
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(out).decode(),
                }))
                self._turn_open = True
                sent += 1
            except Exception:
                break
        if sent:
            LOG.info("Wake oncesi %d parca (%d ms) ses geriye donuk gonderildi",
                     sent, sent * 20)

    async def ensure_openai(self):
        if self.oai is not None:
            return
        if not self.api_key:
            LOG.error("OpenAI API anahtari bos - add-on ayarlarindan gir")
            await self.set_state("error")
            return
        # Talimatlar oturum acilirken BIR KEZ gonderiliyor; katalog eksikse
        # bu oturum boyunca asistan cihazlari bilemez. Acmadan once bir sans daha.
        if not self.ha.katalog_tam and time.time() - self.ha._son_katalog > 20:
            onceki = self.ha.katalog_sayi
            await self.ha.build_catalog(sessiz=True)
            if self.ha.katalog_sayi != onceki:
                LOG.info("Oturum oncesi katalog tazelendi: %d -> %d varlik",
                         onceki, self.ha.katalog_sayi)
        url = f"wss://api.openai.com/v1/realtime?model={self.model}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        LOG.info("OpenAI Realtime oturumu aciliyor (%s)", self.model)
        t0 = time.time()
        self.oai = await websockets.connect(url, max_size=None, **_connect_kwargs(headers))
        self._session_at = time.time()      # bu oturum ne zaman acildi
        self.last_activity = time.time()
        open_ms = int((time.time() - t0) * 1000)
        buf_ms = self._prebuf.maxlen * 20
        LOG.info("Oturum %d ms'de acildi (on tampon %d ms)", open_ms, buf_ms)
        if open_ms > buf_ms * 0.8:
            LOG.warning("Oturum acilisi on tampondan uzun! prebuffer_ms degerini "
                        "artir, yoksa wake word sonrasi soylenenlerin basi kaybolur.")
        self.up = Resampler(DEVICE_RATE, OAI_RATE)
        self.down = Resampler(OAI_RATE, DEVICE_RATE)
        await self.oai.send(json.dumps({
            "type": "session.update",
            "session": {
                "type": "realtime",
                "instructions": self.system_prompt(),
                "output_modalities": ["audio"],
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcm", "rate": OAI_RATE},
                        # Konusma basi/sonunu koprude kendimiz olcuyoruz (asagida
                        # _vad_step); sunucu VAD'i kapali.
                        "turn_detection": None,
                        "transcription": {"model": "whisper-1"},
                    },
                    "output": {
                        "format": {"type": "audio/pcm", "rate": OAI_RATE},
                        "voice": self.voice,
                    },
                },
                "tools": self.tools,
                "tool_choice": "auto",
            },
        }))
        self.oai_task = asyncio.create_task(self.openai_reader())

    async def close_openai(self):
        if self.oai_task:
            # Okuyucu gorev tam da cihaza ses yazarken iptal edilirse
            # WebSocket cercevesi yarim kalir ve cihaz baglantiyi duserdi
            # (logda: uykuya gecisten ~2 sn sonra "Cihaz ayrildi").
            # Once gonderim kilidini al, sonra iptal et.
            async with self._gonderim:
                self.oai_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.gather(self.oai_task,
                                                  return_exceptions=True)), 2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
            self.oai_task = None
        if self.oai:
            try:
                await self.oai.close()
            except Exception:
                pass
            self.oai = None
            self.speaking = False
            self._session_at = 0.0
            self._sleep_at = None        # gecmiste kalmis sayac yeni oturumu oldurmesin
            self._response_active = False
            LOG.info("OpenAI oturumu kapatildi")

    async def openai_reader(self):
        try:
            async for raw in self.oai:
                await self.on_openai_event(json.loads(raw))
        except asyncio.CancelledError:
            raise
        except websockets.ConnectionClosed:
            LOG.info("OpenAI baglantisi kapandi")
            self.oai = None
        except Exception as e:
            LOG.exception("OpenAI okuma hatasi: %s", e)

    async def on_openai_event(self, ev: dict):
        t = ev.get("type", "")
        if t not in ("response.output_audio.delta", "response.audio.delta"):
            LOG.info("OAI olay: %s", t)          # teshis: sunucu ne diyor

        if t == "response.created":
            self._response_active = True

        elif t == "input_audio_buffer.speech_started":
            self.last_activity = time.time()
            await self.set_state("listening")

        elif t == "input_audio_buffer.speech_stopped":
            await self.set_state("thinking")

        elif t in ("response.output_audio.delta", "response.audio.delta"):
            b = base64.b64decode(ev.get("delta", ""))
            if not b:
                return
            if not self.speaking:
                self.speaking = True
                await self.set_state("speaking")
            self.last_activity = time.time()
            pcm = self._out_carry + self.down(b)
            if not pcm:
                return
            # Cihaz 640 baytlik (20 ms) paketlerle calisiyor; artigi bir sonraki
            # deltaya tasi ki tamponda yarim paket kalmasin.
            n = (len(pcm) // 640) * 640
            for i in range(0, n, 640):
                await self.send_device_audio(pcm[i:i + 640])
            self._out_carry = pcm[n:]

        elif t in ("response.output_audio_transcript.done", "response.audio_transcript.done"):
            LOG.info("Asistan: %s", ev.get("transcript", ""))
            self.gecmis_yaz("asistan", ev.get("transcript", ""))

        elif t == "conversation.item.input_audio_transcription.completed":
            _metin = ev.get("transcript", "")
            LOG.info("Kullanici: %s", _metin)
            self.gecmis_yaz("kullanici", _metin)
            await self._hypno_kontrol(_metin)

        elif t == "response.function_call_arguments.done":
            await self.run_tool(ev.get("name", ""), ev.get("call_id", ""),
                                ev.get("arguments", "{}"))

        elif t in ("response.output_audio.done", "response.audio.done"):
            if self._out_carry:                      # kalan artigi tamamla
                await self.send_device_audio(self._out_carry + b"\x00" * (640 - len(self._out_carry)))
                self._out_carry = b""

        elif t == "response.done":
            self._kullanim_kaydet(ev)
            if self._proaktif_bekleyen:
                kaynak, self._proaktif_bekleyen = self._proaktif_bekleyen, None
                cikti = (ev.get("response") or {}).get("output") or []
                sesli = any(
                    p.get("type") in ("audio", "output_audio")
                    for oge in cikti if isinstance(oge, dict)
                    for p in (oge.get("content") or []) if isinstance(p, dict))
                if not sesli:
                    LOG.warning("Proaktif bildirim (%s) SESSIZ gecti - model "
                                "konusmadi. Uretilen oge turleri: %s", kaynak,
                                [o.get("type") for o in cikti
                                 if isinstance(o, dict)] or "yok")
            self._response_active = False
            self.speaking = False
            self.last_activity = time.time()
            self._reset_vad()
            if self._sleep_now:
                self._sleep_now = False
                # Veda cumlesinin sesi cihazda bitsin diye kisa bekleme.
                self._sleep_at = time.time() + 1.5
                await self.set_state("idle", "neutral")
            elif self.follow_up_window <= 0:
                await self.go_to_sleep("takip suresi 0")
            else:
                # Yanit bitti. Kisa bir sure daha dinle (kullanici devam edebilir),
                # kimse konusmazsa uykuya don. Boylece arka plan sesleri
                # dakikalarca modele gitmiyor.
                self._arm_sleep()
                await self.set_state("listening")

        elif t == "error":
            LOG.error("OpenAI hata: %s", json.dumps(ev)[:500])
            await self.set_state("error")

    def _kullanim_dosyasi(self) -> str:
        """CSV'yi /share altina yazar - Samba'dan (\\\\HA_IP\\share) gorulebilir.
        /share yoksa (bagimsiz calisma) state_dir'e duser."""
        if self._kullanim_yol:
            return self._kullanim_yol
        for kok in ("/share/atom_asistan", self.state_dir):
            try:
                os.makedirs(kok, exist_ok=True)
                yol = os.path.join(kok, "token_kullanim.csv")
                with open(yol, "a", encoding="utf-8"):
                    pass
                self._kullanim_yol = yol
                LOG.info("Token kullanim dosyasi: %s", yol)
                return yol
            except Exception:
                continue
        self._kullanim_yol = os.path.join(self.state_dir, "token_kullanim.csv")
        return self._kullanim_yol

    # ------------------------------------------------- token kullanimi
    def _kullanim_kaydet(self, ev: dict):
        """response.done icindeki usage bilgisini loga ve CSV'ye yazar.
        API bunu zaten her yanitta gonderiyor - ek token maliyeti YOK."""
        u = (ev.get("response") or {}).get("usage") or {}
        if not u:
            return
        gd = u.get("input_token_details") or {}
        cd = u.get("output_token_details") or {}
        onb = int(gd.get("cached_tokens") or 0)
        g_metin = int(gd.get("text_tokens") or 0)
        g_ses = int(gd.get("audio_tokens") or 0)
        c_metin = int(cd.get("text_tokens") or 0)
        c_ses = int(cd.get("audio_tokens") or 0)

        # Onbellekli kisim hangi turden geldiyse oradan dus.
        od = gd.get("cached_tokens_details") or {}
        onb_metin = int(od.get("text_tokens") or 0)
        onb_ses = int(od.get("audio_tokens") or 0)
        if not (onb_metin or onb_ses) and onb:
            onb_metin = min(onb, g_metin)          # ayrinti yoksa metin varsay
            onb_ses = onb - onb_metin
        yeni_metin = max(0, g_metin - onb_metin)
        yeni_ses = max(0, g_ses - onb_ses)

        usd = (yeni_metin * FIYAT["metin_giris"]
               + yeni_ses * FIYAT["ses_giris"]
               + (onb_metin + onb_ses) * FIYAT["onbellek"]
               + c_metin * FIYAT["metin_cikis"]
               + c_ses * FIYAT["ses_cikis"]) / 1_000_000

        self._maliyet_toplam += usd
        self._yanit_sayisi += 1
        self.maliyet_ekle(usd)        # gunluk toplam + HA sensoru

        LOG.info("Kullanim: giris %d (metin %d, ses %d, onbellek %d) | "
                 "cikis %d (metin %d, ses %d) | $%.4f | oturum toplami $%.4f",
                 int(u.get("input_tokens") or 0), g_metin, g_ses, onb,
                 int(u.get("output_tokens") or 0), c_metin, c_ses,
                 usd, self._maliyet_toplam)

        try:
            yol = self._kullanim_dosyasi()
            # DIKKAT: _kullanim_dosyasi() yazilabilirligi denerken dosyayi
            # olusturuyor. "var mi" diye bakarsak baslik satiri hic yazilmaz;
            # o yuzden BOYUTA bakiyoruz.
            try:
                yeni_dosya = os.path.getsize(yol) == 0
            except OSError:
                yeni_dosya = True
            with open(yol, "a", encoding="utf-8") as f:
                if yeni_dosya:
                    f.write("tarih,giris_toplam,giris_metin,giris_ses,onbellek,"
                            "cikis_toplam,cikis_metin,cikis_ses,usd\n")
                f.write("%s,%d,%d,%d,%d,%d,%d,%d,%.6f\n" % (
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    int(u.get("input_tokens") or 0), g_metin, g_ses, onb,
                    int(u.get("output_tokens") or 0), c_metin, c_ses, usd))
        except Exception as e:
            LOG.warning("Kullanim dosyasi yazilamadi: %s", e)

    async def run_tool(self, name: str, call_id: str, args_json: str):
        try:
            args = json.loads(args_json or "{}")
        except json.JSONDecodeError:
            args = {}
        LOG.info("Tool: %s %s", name, args)
        try:
            if name == "ha_call_service":
                result = await self.ha.call_service(args.get("domain", ""),
                                                    args.get("service", ""),
                                                    args.get("entity_id"),
                                                    args.get("data"))
                if isinstance(result, dict) and result.get("ok"):
                    await self.set_state("success")   # kisa yesil onay
            elif name == "ha_get_state":
                await self.set_state("searching")     # gozler tarama yapsin
                result = await self.ha.get_state(args.get("entity_id", ""))
            elif name == "show_on_pc":
                text = (args.get("prompt") or "").strip()
                if not text:
                    result = {"error": "bos metin"}
                elif not (self.pc_show and self.pc_topic):
                    result = {"error": "PC'ye yansitma kapali"}
                elif self.pc_transport == "atom_desk":
                    # Atom Desk uygulamasi: duz JSON. Komut satiri araya
                    # girmedigi icin tirnak kacisi ve uzunluk sinirlamasi yok.
                    safe = re.sub(r"[ \t]+", " ", text).strip()
                    if len(safe) > 1000:
                        safe = safe[:997].rsplit(" ", 1)[0] + "..."
                    provider = (args.get("provider") or "").lower()
                    if provider not in ("claude", "chatgpt"):
                        provider = None
                    payload = {"prompt": safe}
                    if provider:
                        payload["provider"] = provider
                    await self.ha.mqtt_publish(
                        self.pc_topic, json.dumps(payload, ensure_ascii=False))
                    LOG.info("Atom Desk'e gonderildi (%s, %d karakter): %s",
                             provider or "varsayilan", len(safe), safe)
                    await self.set_state("pc")        # gozler sol alta baksin
                    result = {"ok": True, "provider": provider or "varsayilan"}
                else:
                    # HASS.Agent payload'u executor'a "oldugu gibi" argüman olarak verir.
                    # Metni tek bir tirnakli argümana cevirip komut onekinin sonuna ekliyoruz.
                    # Tek satira indir, tirnaklari sadelestir, uzunlugu sinirla:
                    # uzun metinler Windows komut satirinda kiriliyor.
                    safe = re.sub(r"\s+", " ", text).replace('"', "'").strip()
                    if len(safe) > 300:
                        safe = safe[:297].rsplit(" ", 1)[0] + "..."
                    mode = f' -Mode {self.pc_mode}' if self.pc_mode else ""
                    payload = (f'{self.pc_prefix}{mode} "{safe}"'
                               if self.pc_prefix else f'"{safe}"')
                    await self.ha.mqtt_publish(self.pc_topic, payload)
                    LOG.info("PC'ye gonderildi (%d karakter): %s", len(safe), safe)
                    result = {"ok": True}
            elif name == "set_volume":
                lvl = max(0, min(100, int(args.get("level", 70))))
                await self.send_device_text({"type": "volume", "value": lvl})
                self.volume = lvl
                result = {"ok": True, "level": lvl}
            elif name == "rahatsiz_etme":
                result = await self.dnd_ayarla(
                    bool(args.get("acik")), args.get("dakika"), kaynak="sesli")
            elif name == "hipnoz_modu":
                result = await self.hypno_ayarla(
                    bool(args.get("acik")), kaynak="sesli")
            elif name == "ayar_degistir":
                result = await self.ayar_degistir(args.get("ad", ""),
                                                  args.get("deger", ""))
            elif name == "ayar_oku":
                result = self.ayar_oku(args.get("ad", ""))
            elif name == "ayar_geri_al":
                result = await self.ayar_geri_al()
            elif name == "set_face":
                await self.send_device_text({"type": "emotion",
                                             "value": args.get("emotion", "neutral")})
                result = {"ok": True}
            elif name == "sleep":
                # Model konusmayi bitirdi. Yanitin sesi cihaza gitsin diye
                # hemen kapatmiyoruz; response.done'dan sonra uyku sayaci
                # 1 saniyeye cekiliyor.
                # "Uyu" dendiginde eslik suresini BEKLEME: kullanici
                # acikca istedi.
                self._sleep_now = True
                self._eslik_at = None
                result = {"ok": True}
            elif name == "hatirla":
                result = self.hafiza_ekle(args.get("bilgi", ""))
                if result.get("ok"):
                    await self.set_state("success")
            elif name == "unut":
                result = self.hafiza_sil(args.get("arama", ""))
            elif name == "hafizayi_oku":
                result = {"hafiza": self.hafiza_listesi()}
                await self.set_state("searching")
            elif name == "zamanlayici_kur":
                result = self.timer_kur(args.get("saniye", 0),
                                        args.get("etiket", ""))
                if result.get("ok"):
                    await self._timer_ekrani_guncelle(zorla=True)
                    await self.set_state("success")
            elif name == "gecmisi_oku":
                result = self.gecmis_oku(args.get("gun_once", 0),
                                         args.get("arama", ""),
                                         args.get("adet", 30))
                await self.set_state("searching")
            elif name == "hatirlatici_kur":
                result = self.hatirlatici_kur(
                    args.get("saat"), args.get("dakika", 0),
                    args.get("tekrar", "tek"), args.get("gun_sonra", 0),
                    args.get("not", ""))
                if result.get("ok"):
                    await self.set_state("success")
            elif name == "hatirlaticilari_listele":
                result = self.hatirlatici_liste()
            elif name == "hatirlatici_iptal":
                result = self.hatirlatici_iptal(args.get("id"))
            elif name == "zamanlayicilari_listele":
                result = self.timer_liste()
            elif name == "zamanlayici_iptal":
                result = self.timer_iptal(args.get("id"))
                if result.get("ok"):
                    await self._timer_ekrani_guncelle(zorla=True)
            else:
                result = {"error": f"bilinmeyen fonksiyon: {name}"}
        except Exception as e:
            result = {"error": str(e)}
        LOG.info("Tool sonucu: %s", result)

        if self.oai is None:
            return
        await self.oai.send(json.dumps({
            "type": "conversation.item.create",
            "item": {"type": "function_call_output",
                     "call_id": call_id,
                     "output": json.dumps(result, ensure_ascii=False)},
        }))
        if not self._response_active:
            await self.oai.send(json.dumps({"type": "response.create"}))



    # ====================== KONUSMA GECMISI ============================
    # Oturum kapaninca her sey uctugu icin "az once ne dedim", "bugun ne
    # konustuk" cevaplanamiyordu. Her tur gunluk bir JSONL dosyasina
    # yaziliyor; model gecmisi_oku araciyla okuyabiliyor.
    def _gecmis_yol(self, gun=None):
        gun = gun or time.strftime("%Y-%m-%d")
        return os.path.join(self.gecmis_dizin, f"{gun}.jsonl")

    def gecmis_yaz(self, kim: str, metin: str):
        if not self.gecmis_on:
            return
        metin = " ".join(str(metin or "").split()).strip()
        if not metin:
            return
        try:
            os.makedirs(self.gecmis_dizin, exist_ok=True)
            with open(self._gecmis_yol(), "a", encoding="utf-8") as f:
                f.write(json.dumps({"t": time.strftime("%H:%M"),
                                    "kim": kim, "metin": metin},
                                   ensure_ascii=False) + "\n")
        except Exception as e:
            LOG.debug("Gecmis yazilamadi: %s", e)

    def _gecmis_oku_dosya(self, gun):
        satirlar = []
        try:
            with open(self._gecmis_yol(gun), "r", encoding="utf-8") as f:
                for l in f:
                    l = l.strip()
                    if not l:
                        continue
                    try:
                        satirlar.append(json.loads(l))
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            pass
        except Exception as e:
            LOG.warning("Gecmis okunamadi (%s): %s", gun, e)
        return satirlar

    def gecmis_oku(self, gun_once=0, arama="", adet=30):
        """gun_once: 0 = bugun, 1 = dun. arama verilirse filtreler."""
        try:
            gun_once = max(0, min(30, int(gun_once or 0)))
        except (TypeError, ValueError):
            gun_once = 0
        gun = time.strftime("%Y-%m-%d",
                            time.localtime(time.time() - gun_once * 86400))
        kayit = self._gecmis_oku_dosya(gun)
        arama = _sadelestir(arama).strip()
        if arama:
            # Turkcede son unsuz yumusuyor: "isik" kelimesi metinde
            # "isigini" olarak geciyor ve duz alt-dize aramasi kaciriyor.
            # Cozum: 4+ harfli kelimeleri son harfi atilmis haliyle de
            # dene. Butun kelimelerin eslesmesi sart oldugu icin bu
            # gevsetme yanlis sonuc uretmiyor.
            def eslesir(k, metin):
                return k in metin or (len(k) >= 4 and k[:-1] in metin)
            kelimeler = [k for k in arama.split() if k]
            kayit = [x for x in kayit
                     if all(eslesir(k, _sadelestir(x.get("metin", "")))
                            for k in kelimeler)]
        try:
            adet = max(1, min(100, int(adet or 30)))
        except (TypeError, ValueError):
            adet = 30
        # Sondan al: "az once ne dedim" en sik sorulan sey.
        kesit = kayit[-adet:]
        if not kesit:
            return {"gun": gun, "adet": 0,
                    "not": "o gune ait kayit yok" if not arama
                           else "eslesme yok"}
        return {"gun": gun, "adet": len(kesit), "toplam": len(kayit),
                "konusma": [f"{x['t']} {x['kim']}: {x['metin']}" for x in kesit]}

    def gecmis_temizle(self):
        """gecmis_gun_sayisi'ndan eski dosyalari siler."""
        if not self.gecmis_on or self.gecmis_gun <= 0:
            return
        try:
            sinir = time.time() - self.gecmis_gun * 86400
            for ad in os.listdir(self.gecmis_dizin):
                if not ad.endswith(".jsonl"):
                    continue
                yol = os.path.join(self.gecmis_dizin, ad)
                try:
                    if os.path.getmtime(yol) < sinir:
                        os.remove(yol)
                        LOG.info("Eski konusma kaydi silindi: %s", ad)
                except OSError:
                    continue
        except FileNotFoundError:
            pass
        except Exception as e:
            LOG.debug("Gecmis temizligi basarisiz: %s", e)

    # =================== MUTLAK SAATLI HATIRLATICILAR ==================
    # Zamanlayicidan ayri tutuluyor: o geri sayim (kac saniye kaldi),
    # bu takvim (hangi saatte). Ikisini birlestirmek, tekrarli olanlarda
    # "kalan sure" kavramini anlamsizlastiriyordu.
    HAT_GUNLER = {"gunluk": (0, 1, 2, 3, 4, 5, 6),
                  "hafta_ici": (0, 1, 2, 3, 4),
                  "hafta_sonu": (5, 6)}

    def _hat_yukle(self):
        try:
            with open(self.hat_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.hatirlaticilar = {int(k): v for k, v in (d.get("liste") or {}).items()}
            self._hat_sonraki_id = int(d.get("sonraki_id") or 1)
        except FileNotFoundError:
            pass
        except Exception as e:
            LOG.warning("Hatirlaticilar okunamadi: %s", e)

    def _hat_kaydet(self):
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            tmp = self.hat_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"liste": self.hatirlaticilar,
                           "sonraki_id": self._hat_sonraki_id}, f,
                          ensure_ascii=False, indent=1)
            os.replace(tmp, self.hat_path)
        except Exception as e:
            LOG.warning("Hatirlaticilar yazilamadi: %s", e)

    def _hat_sonraki_an(self, h, simdi=None):
        """Bir sonraki calma anini epoch olarak hesaplar.
        Tekrarsizsa gecmiste kaldiysa None doner (silinmeli)."""
        simdi = time.time() if simdi is None else simdi
        sa, dk = int(h["saat"]), int(h["dakika"])
        tekrar = h.get("tekrar", "tek")
        yst = time.localtime(simdi)
        # Bugunun o saatteki epoch'u
        def an(gun_ofs):
            t = time.localtime(simdi + gun_ofs * 86400)
            return time.mktime((t.tm_year, t.tm_mon, t.tm_mday, sa, dk, 0,
                                0, 0, -1))
        if tekrar == "tek":
            hedef = float(h.get("an") or 0)
            return hedef if hedef > 0 else None
        gunler = self.HAT_GUNLER.get(tekrar, self.HAT_GUNLER["gunluk"])
        for ofs in range(0, 8):
            t = time.localtime(simdi + ofs * 86400)
            if t.tm_wday not in gunler:
                continue
            a = an(ofs)
            if a > simdi + 1:
                return a
        return None

    def hatirlatici_kur(self, saat, dakika=0, tekrar="tek", gun_sonra=0, not_=""):
        try:
            saat, dakika = int(saat), int(dakika or 0)
        except (TypeError, ValueError):
            return {"error": "saat/dakika sayi olmali"}
        if not (0 <= saat <= 23 and 0 <= dakika <= 59):
            return {"error": "saat 0-23, dakika 0-59 olmali"}
        tekrar = tekrar if tekrar in ("tek", "gunluk", "hafta_ici", "hafta_sonu") else "tek"
        not_ = " ".join(str(not_ or "").split()).strip()
        if not not_:
            return {"error": "hatirlatilacak sey bos"}
        if len(self.hatirlaticilar) >= 20:
            return {"error": "en fazla 20 hatirlatici olabilir"}

        simdi = time.time()
        h = {"saat": saat, "dakika": dakika, "tekrar": tekrar, "not": not_}
        if tekrar == "tek":
            ofs = max(0, int(gun_sonra or 0))
            t = time.localtime(simdi + ofs * 86400)
            an = time.mktime((t.tm_year, t.tm_mon, t.tm_mday, saat, dakika, 0, 0, 0, -1))
            # Bugun icin verilen saat gecmisse yarina at - "9'da hatirlat"
            # denince gecmis bir ani kurmak sessizce hic calmamak demek.
            if ofs == 0 and an <= simdi + 5:
                an += 86400
            h["an"] = an
        else:
            h["an"] = self._hat_sonraki_an(h, simdi)
        if not h["an"]:
            return {"error": "gecerli bir zaman bulunamadi"}

        hid = self._hat_sonraki_id
        self._hat_sonraki_id += 1
        self.hatirlaticilar[hid] = h
        self._hat_kaydet()
        ne = time.strftime("%d.%m %H:%M", time.localtime(h["an"]))
        LOG.info("Hatirlatici #%d kuruldu: %s (%s) - %s", hid, ne, tekrar, not_)
        return {"ok": True, "id": hid, "ne_zaman": ne, "tekrar": tekrar,
                "not": not_}

    def hatirlatici_liste(self):
        out = []
        for hid, h in sorted(self.hatirlaticilar.items(),
                             key=lambda kv: kv[1].get("an") or 0):
            out.append({"id": hid,
                        "ne_zaman": time.strftime("%d.%m %H:%M",
                                                  time.localtime(h["an"])),
                        "saat": f"{h['saat']:02d}:{h['dakika']:02d}",
                        "tekrar": h.get("tekrar", "tek"),
                        "not": h.get("not", "")})
        return {"hatirlaticilar": out, "adet": len(out)}

    def hatirlatici_iptal(self, hid=None):
        if hid is None:
            if len(self.hatirlaticilar) != 1:
                return {"error": "id lazim; birden fazla hatirlatici var"}
            hid = next(iter(self.hatirlaticilar))
        h = self.hatirlaticilar.pop(int(hid), None)
        if h is None:
            return {"error": f"#{hid} bulunamadi"}
        self._hat_kaydet()
        LOG.info("Hatirlatici #%s iptal edildi", hid)
        return {"ok": True, "iptal_edilen": int(hid), "not": h.get("not", "")}

    async def hatirlatici_watchdog(self):
        """Zamani gelenleri calistirir. Tekrarlilar bir sonraki gune
        kurulur, tek seferlikler silinir."""
        while True:
            await asyncio.sleep(20)
            try:
                simdi = time.time()
                for hid in sorted(self.hatirlaticilar):
                    h = self.hatirlaticilar.get(hid)
                    if not h or (h.get("an") or 0) > simdi:
                        continue
                    # Cok geride kalmislari (kopru kapaliyken gecen) calma:
                    # sabah 8 hatirlaticisi aksam 6'da calmasin.
                    gecikme = simdi - h["an"]
                    calsin = gecikme <= self.hat_gecikme_sn
                    if h.get("tekrar", "tek") == "tek":
                        self.hatirlaticilar.pop(hid, None)
                    else:
                        h["an"] = self._hat_sonraki_an(h, simdi + 1)
                        if not h["an"]:
                            self.hatirlaticilar.pop(hid, None)
                    self._hat_kaydet()
                    if not calsin:
                        LOG.info("Hatirlatici #%d %d dk gecikmis, atlandi: %s",
                                 hid, int(gecikme // 60), h.get("not", ""))
                        continue
                    LOG.info("Hatirlatici #%d doldu: %s", hid, h.get("not", ""))
                    await self.proaktif_konus(
                        "Kurdugun hatirlatici zamani geldi. Konu: "
                        f"{h.get('not','')}. Kullaniciya bunu kisa ve dogal "
                        "bir cumleyle hatirlat.",
                        kaynak=f"hatirlatici#{hid}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                LOG.warning("Hatirlatici watchdog hatasi: %s", e)

    # ------------------------------------------------------ zamanlayici
    async def idle_watchdog(self):
        while True:
            await asyncio.sleep(0.5)
            if self.oai is None:
                continue
            now = time.time()

            # Yanit surerken normal sayaclar islemez. Ama response.done
            # hic gelmezse (kopuk akis, sunucu hatasi) bu bayraklar takili
            # kalip watchdog'u tamamen devre disi birakiyordu.
            if self.speaking or self._response_active:
                if now - self.last_activity > max(self.idle_timeout, 60.0):
                    LOG.warning("Yanit %.0f sn once basladi ve hic bitmedi; "
                                "durum sifirlaniyor", now - self.last_activity)
                    self.speaking = False
                    self._response_active = False
                    await self.go_to_sleep("yanit takildi")
                continue

            # Sayac bir sekilde silinmisse yeniden kur - "hic uyumama"
            # durumunun kalan tek yolu buydu.
            if (self._sleep_at is None and not self._in_speech
                    and not self._dinleme_kapali):
                self._arm_sleep()

            # VAD "hala konusuluyor" diye takili kalirsa uyku hic gelmez.
            # Ortam gurultusu esigin uzerindeyse boyle oluyor: zorla sifirla
            # ve nedenini logla, boylece esik ayari gorunur olsun.
            if (self._in_speech and self._sleep_at is not None
                    and now > self._sleep_at + self.follow_up_window):
                LOG.warning("VAD surekli 'konusuluyor' diyor "
                            "(gurultu tabani %.0f, esik %.0f) - sifirlaniyor. "
                            "Suruyorsa vad_threshold_mult'i artir.",
                            self._noise, self._threshold())
                self._reset_vad()

            # Yeni acilan oturuma dokunma. Oturum kurulurken (prebuffer
            # akitilirken) hicbir sayac onu olduremesin - iki ayri hata
            # tam da bu pencerede olusmustu.
            if now - self._session_at < 3.0:
                continue

            # 1) Yanittan sonraki takip suresi doldu ve kimse konusmadi.
            if (self._sleep_at is not None and now > self._sleep_at
                    and not self._in_speech):
                # Hipnoz modu suresi dolana kadar dinlemeyi birakma: yoksa
                # "normale don" duyulmuyor ve modun tek cikisi butona
                # kaliyor. Mod zaten hypno_saniye ile sinirli.
                if self.hypno_aktif():
                    self._arm_sleep()
                    continue
                await self.dinlemeyi_durdur("takip suresi doldu")
                continue
            # 1b) Eslik suresi doldu: artik gercekten uyu.
            #     Oturum zaten (2) ile kapaniyor; buradaki is yalnizca
            #     EKRANI uykuya almak.
            if (self._eslik_at is not None and self.eslik_dk > 0
                    and now - self._eslik_at > self.eslik_dk * 60):
                self._eslik_at = None
                LOG.info("Eslik suresi doldu (%d dk) - ekran uykuya aliniyor",
                         self.eslik_dk)
                await self.set_state("sleep", "neutral")

            # 2) Emniyet freni: hicbir sey olmadan cok uzun sure gecti.
            #    Oturumun kendisi idle_timeout'tan genc ise bu kural islemez -
            #    yoksa daha acilirken "bosta" sayilip kapatiliyordu.
            if (now - self._session_at > self.idle_timeout
                    and now - self.last_activity > self.idle_timeout):
                await self.go_to_sleep("bosta kalindi")

    # ================= ATOM DESK SURUM BILDIRIMI =======================
    #
    #  NEDEN KOPRU BAKIYOR: Windows uygulamasi ac-kapa oluyor, PC uyuyor,
    #  aglar degisiyor. Kopru ise 7/24 ayakta ve zaten MQTT'de. Kontrolu
    #  buraya alinca uygulama tarafinda periyodik GitHub yoklamasi, zaman
    #  asimi yonetimi ve "internet var mi" kontrolu gerekmiyor.
    #
    #  MESAJ RETAINED: uygulama ne zaman baglanirsa baglansin son surumu
    #  aninda aliyor. Retained olmasaydi acilista bir sonraki kontrole
    #  kadar (saatlerce) hicbir sey duymazdi.
    #
    #  Kopru GUNCELLEMEYI KENDI YAPMIYOR - sadece haber veriyor. PC'de
    #  dosya degistirme yetkisi koprude degil, uygulamanin kendisinde.

    _DESK_DESEN = re.compile(r'^SURUM\s*=\s*"([^"]+)"', re.M)

    def _desk_konu(self) -> str:
        return self.ATOM_KOK + "/desk/guncelleme"

    async def _desk_surum_oku(self) -> Optional[str]:
        """GitHub'daki pc/atom_desk/surum.py dosyasindan surumu okur."""
        url = (f"https://raw.githubusercontent.com/{self.desk_depo}"
               f"/{self.desk_dal}/{self.desk_surum_yolu}")
        try:
            async with ClientSession(timeout=ClientTimeout(total=15)) as s:
                async with s.get(url) as r:
                    if r.status == 404:
                        # En olasi sebep: depo private. raw.githubusercontent
                        # kimlik bilgisi olmadan private depoya 404 doner.
                        # raw.githubusercontent kimlik bilgisi almadigi icin
                        # PRIVATE depoya da 404 doner. Surum isareti bu yuzden
                        # public add-on deposunda duruyor.
                        LOG.warning(
                            "Atom Desk surum dosyasi bulunamadi (404): %s\n"
                            "  desk_depo PUBLIC bir depo mu, dal (%s) ve yol "
                            "(%s) dogru mu?",
                            url, self.desk_dal, self.desk_surum_yolu)
                        return None
                    if r.status != 200:
                        LOG.warning("Atom Desk surumu okunamadi (HTTP %s)", r.status)
                        return None
                    metin = await r.text()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Internet kesintisi normal; bir sonraki turda tekrar denenecek.
            LOG.debug("Atom Desk surum kontrolu basarisiz: %s: %s",
                      type(e).__name__, e)
            return None

        m = self._DESK_DESEN.search(metin)
        if not m:
            LOG.warning("surum.py icinde SURUM satiri bulunamadi")
            return None
        return m.group(1).strip()

    async def desk_surum_watchdog(self):
        """Periyodik olarak son Atom Desk surumunu MQTT'ye yazar."""
        if not self.desk_guncelleme_on:
            return
        if not self.notify_on:
            LOG.info("Atom Desk surum bildirimi acik ama MQTT (notify_enabled) "
                     "kapali; bildirim yayinlanamaz.")
            return
        # Acilista MQTT'nin baglanmasini bekle; ilk yayin bosa gitmesin.
        await asyncio.sleep(45)
        while True:
            surum = await self._desk_surum_oku()
            if surum:
                yuk = {"surum": surum,
                       "depo": self.desk_depo,
                       "dal": self.desk_dal,
                       "bakildi": time.strftime("%Y-%m-%dT%H:%M:%S")}
                if self._hass_yayinla(self._desk_konu(), yuk):
                    if surum != self._desk_son_surum:
                        LOG.info("Atom Desk son surumu: %s -> %s yayinlandi",
                                 surum, self._desk_konu())
                        self._desk_son_surum = surum
                else:
                    # MQTT henuz hazir degilse bir sonraki turda tekrar
                    # denensin diye son surumu KAYDETMIYORUZ.
                    LOG.debug("MQTT hazir degil, Atom Desk surumu sonra yazilacak")
            await asyncio.sleep(max(1, self.desk_kontrol_saat) * 3600)

    async def run(self):
        # Ilk deneme burada: hizli yolda katalog hazir olsun. Eksik kalirsa
        # arka planda tamamlanmaya devam eder, cihaz beklemek zorunda kalmaz.
        await self.ha.build_catalog()
        if not self.ha.katalog_tam:
            asyncio.create_task(self.ha.katalog_hazir_olana_kadar())
        asyncio.create_task(self.ha.katalog_yenileyici())
        host = self.cfg.get("listen_host", "0.0.0.0")
        port = int(self.cfg.get("listen_port", 8765))
        self._loop = asyncio.get_running_loop()
        asyncio.create_task(self.idle_watchdog())
        if self.timers_on:
            asyncio.create_task(self.timer_watchdog())
        # Nobetciler KOSULSUZ basliyor; acik/kapali karari iclerinde.
        # Eskiden burada bayraga bakiliyordu ve ayar sesle acilinca nobetci
        # hic baslamadigi icin yeniden baslatmadan devreye girmiyordu.
        asyncio.create_task(self.ortam_watchdog())
        asyncio.create_task(self.dans_watchdog())
        asyncio.create_task(self.desk_surum_watchdog())
        if self.gecmis_on:
            self.gecmis_temizle()
            LOG.info("Konusma gecmisi: acik (%d gun saklaniyor)", self.gecmis_gun)
        if self.hat_on:
            self._hat_yukle()
            asyncio.create_task(self.hatirlatici_watchdog())
            if self.hatirlaticilar:
                LOG.info("Hatirlatici: %d kayit yuklendi", len(self.hatirlaticilar))
        self._maliyet_yukle()
        self.mqtt_baslat()
        LOG.info("Kopru surumu: %s", BRIDGE_VERSION)
        LOG.info("Eslik modu: %s",
                 f"{self.eslik_dk} dakika uyanik kalir"
                 if self.eslik_dk > 0 else "kapali (dinleme bitince uyur)")
        LOG.info("Sesle degistirilebilir ayar: %d adet (kimlik bilgileri ve "
                 "guvenlik ayarlari haric)", len(AYAR_KAYDI))
        LOG.info("Hafiza: %d sabit + %d ogrenilen satir",
                 len(self.hafiza_sabit), len(self.hafiza_ogrenilen))
        LOG.info("Zamanlayici: %s   HA bildirimleri: %s",
                 "acik" if self.timers_on else "kapali",
                 f"acik ({self.notify_topic})" if self.notify_on else "kapali")
        # Ortam modunun ayarini ACIKCA yaz. Ilk surumde yazmiyordu ve
        # ekranda hava cikmayinca "ayar hic gelmemis mi, varlik mi
        # okunamiyor" sorusu logdan cevaplanamiyordu.
        if not self.dans_on:
            LOG.info("Dans: kapali")
        elif self.dans_kaynak == "medya":
            LOG.info("Dans: acik   kaynak=HA medya oynatici (%s)   "
                     "tempo bulunamazsa %d BPM%s",
                     ", ".join(self.dans_medya) if self.dans_medya
                     else "tum media_player varliklari",
                     self.dans_bpm,
                     "   TESHIS ACIK" if self.dans_teshis else "")
        else:
            LOG.info("Dans: acik   kaynak=mikrofon (cihaz kendi karar verir)%s",
                     "   TESHIS ACIK" if self.dans_teshis else "")
        if not self.ortam_on:
            LOG.info("Ortam modu: kapali")
        elif not self.ortam_hava_goster:
            LOG.info("Ortam modu: acik   hava durumu KAPALI, yalniz saat")
        elif not self.ortam_hava_varlik:
            LOG.info("Ortam modu: acik   hava varligi ayarlanmamis, "
                     "HA'daki ilk weather varligi otomatik secilecek")
        else:
            LOG.info("Ortam modu: acik   hava=%s   sicaklik=%s   her %d sn",
                     self.ortam_hava_varlik,
                     self.ortam_isi_varlik or "(hava varligindan)",
                     self.ortam_periyot)
        LOG.info("Kopru dinlemede: ws://%s:%d/device", host, port)
        async with websockets.serve(self.handle_device, host, port,
                                    max_size=None, ping_interval=20,
                                    max_queue=256):
            await asyncio.Future()


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    lvl = str(cfg.get("log_level", "info")).upper()
    logging.getLogger().setLevel(getattr(logging, lvl, logging.INFO))
    asyncio.run(Bridge(cfg).run())


if __name__ == "__main__":
    main()
