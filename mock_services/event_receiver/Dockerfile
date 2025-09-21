FROM python:3.11-slim

WORKDIR /app

# 安装基本工具
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY main.py .

# 创建数据目录
RUN mkdir -p /app/data

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

CMD ["python", "main.py"]
