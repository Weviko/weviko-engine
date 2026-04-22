FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-nanum \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos "" pwuser && \
    mkdir -p /home/pwuser/app && \
    chown -R pwuser:pwuser /home/pwuser

WORKDIR /home/pwuser/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=pwuser:pwuser . .

ENV PYTHONUNBUFFERED=1

USER pwuser

CMD ["python", "main.py"]
