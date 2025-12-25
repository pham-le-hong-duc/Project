FROM apache/airflow:2.10.5-python3.11

# ======================================
# STAGE 1: Cài đặt System Dependencies (User Root)
# ======================================
USER root

# Cài đặt OpenJDK 17 (Cần cho PySpark) và dọn dẹp cache ngay lập tức
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ======================================
# STAGE 2: Cài đặt Python Libraries (User Airflow)
# ======================================
USER airflow

COPY requirements.txt /requirements.txt

# --- KHẮC PHỤC LỖI MẤT FILE AIRFLOW TẠI ĐÂY ---
# Định nghĩa URL chứa file constraints chuẩn của Airflow 2.10.5 + Python 3.11
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 1. Upgrade pip
# 2. Cài đặt các thư viện trong requirements.txt KÈM THEO file constraint.
#    Việc này buộc pip giữ nguyên version Airflow gốc, không được phép gỡ ra.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"