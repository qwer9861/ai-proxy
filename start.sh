#!/bin/bash
# 启动代理服务并自动设置端口公开

cd /workspaces/ai-proxy

# 后台启动 node 服务
echo "[$(date)] Starting AI Proxy..." > /tmp/startup.log
nohup node server.js > /tmp/proxy.log 2>&1 &

# 等待端口监听
echo "[$(date)] Waiting for port 8080..." >> /tmp/startup.log
for i in $(seq 1 30); do
  if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "[$(date)] Port 8080 is up" >> /tmp/startup.log
    break
  fi
  sleep 1
done

# 尝试用 gh 设置端口公开
echo "[$(date)] Setting port visibility to public..." >> /tmp/startup.log
gh codespace ports visibility 8080:public -c "$CODESPACE_NAME" >> /tmp/startup.log 2>&1
echo "[$(date)] Done with exit code $?" >> /tmp/startup.log

# 也尝试用 API 方式（备用）
if [ -n "$GITHUB_TOKEN" ]; then
  echo "[$(date)] Trying API method..." >> /tmp/startup.log
  curl -s -X PATCH \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/user/codespaces/$CODESPACE_NAME/ports/8080" \
    -d '{"visibility":"public"}' >> /tmp/startup.log 2>&1
  echo "" >> /tmp/startup.log
fi

echo "[$(date)] Startup script finished" >> /tmp/startup.log
