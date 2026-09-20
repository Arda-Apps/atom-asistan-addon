FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir \
      "websockets>=12.0" \
      "aiohttp>=3.9" \
      "numpy>=1.24" \
      "soxr>=0.3.7" \
      "paho-mqtt>=1.6"

# --- Wake word ("Hey Jarvis") -------------------------------------------
# openwakeword, Linux'ta zorunlu bagimlilik olarak tflite-runtime istiyor;
# o paketin Python 3.12 + aarch64 wheel'i yok. Biz onnx motorunu kullandigimiz
# icin tflite gerekmiyor -> bagimliliklari elle kurup paketi --no-deps aliyoruz.
# Wake word istemiyorsan asagidaki iki satiri yorum yap, imaj ~250 MB kuculur.
RUN pip install --no-cache-dir "onnxruntime>=1.17" "scipy" "scikit-learn" "requests" "tqdm"
RUN pip install --no-cache-dir --no-deps "openwakeword>=0.6.0"
# Modelleri imaja gom, her acilista tekrar indirmesin
RUN python -c "from openwakeword import utils; utils.download_models(['hey_jarvis','alexa','hey_mycroft','ok_nabu'])"
# ------------------------------------------------------------------------

COPY bridge.py /opt/bridge.py

CMD ["python", "/opt/bridge.py"]
