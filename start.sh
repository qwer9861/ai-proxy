#!/bin/bash
# 启动代理服务并自动设置端口公开

cd /workspaces/ai-proxy
LOG=/tmp/startup.log

echo "[$(date)] === Startup script begin ===" > $LOG
echo "[$(date)] CODESPACE_NAME=$CODESPACE_NAME" >> $LOG

# 后台启动 node 服务
echo "[$(date)] Starting AI Proxy..." >> $LOG
nohup node server.js > /tmp/proxy.log 2>&1 &

# 等待端口监听
echo "[$(date)] Waiting for port 8080..." >> $LOG
for i in $(seq 1 30); do
  if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "[$(date)] Port 8080 is up" >> $LOG
    break
  fi
  sleep 1
done

# 等待 gh 认证就绪
echo "[$(date)] Waiting for gh auth..." >> $LOG
for i in $(seq 1 30); do
  if gh auth status > /dev/null 2>&1; then
    echo "[$(date)] gh auth ready" >> $LOG
    break
  fi
  sleep 2
done

# 重试设置端口公开
echo "[$(date)] Setting port visibility to public..." >> $LOG
for i in $(seq 1 15); do
  result=$(gh codespace ports visibility 8080:public -c "$CODESPACE_NAME" 2>&1)
  exit_code=$?
  echo "[$(date)] Attempt $i: exit=$exit_code, result=$result" >> $LOG
  if [ $exit_code -eq 0 ]; then
    echo "[$(date)] Port set to public successfully!" >> $LOG
    break
  fi
  sleep 3
done

echo "[$(date)] === Startup script end ===" >> $LOG
