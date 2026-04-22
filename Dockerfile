FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-nanum \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

RUN if ! id -u pwuser >/dev/null 2>&1; then \
        adduser --disabled-password --gecos "" pwuser; \
    fi && \
    mkdir -p /home/pwuser/app && \
    chown -R pwuser:pwuser /home/pwuser/app

WORKDIR /home/pwuser/app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r requirements.txt

COPY --chown=pwuser:pwuser main.py ./main.py
COPY --chown=pwuser:pwuser streamlit_app.py ./streamlit_app.py
COPY --chown=pwuser:pwuser streamlit_services.py ./streamlit_services.py
COPY --chown=pwuser:pwuser weviko_factory.py ./weviko_factory.py

ENV PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    PORT=10000

USER pwuser

EXPOSE 10000

CMD ["sh", "-c", "streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port ${PORT:-10000}"]
