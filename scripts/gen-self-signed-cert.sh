#!/usr/bin/env bash
# 生成 rfrp 自签证书（控制链路/工作连接 + vhost）。
# 用法：scripts/gen-self-signed-cert.sh [输出目录]
set -euo pipefail

OUT_DIR="${1:-examples}"
mkdir -p "$OUT_DIR"

# 控制链路 / 工作连接证书
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$OUT_DIR/key.pem" \
  -out "$OUT_DIR/cert.pem" \
  -days 3650 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=digitalSignature,keyEncipherment" >/dev/null 2>&1

# 客户端信任用 CA（自签场景直接信任服务端证书）
cp "$OUT_DIR/cert.pem" "$OUT_DIR/ca.pem"

# HTTPS vhost 证书
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$OUT_DIR/vhost-key.pem" \
  -out "$OUT_DIR/vhost-cert.pem" \
  -days 3650 \
  -subj "/CN=dev.example.com" \
  -addext "subjectAltName=DNS:dev.example.com,DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=digitalSignature,keyEncipherment" >/dev/null 2>&1

chmod 600 "$OUT_DIR/key.pem" "$OUT_DIR/vhost-key.pem"

echo "generated certificates in $OUT_DIR:"
ls -l "$OUT_DIR"/{cert.pem,key.pem,ca.pem,vhost-cert.pem,vhost-key.pem}
