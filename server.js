const https = require('https');
const http = require('http');

const UPSTREAM = 'https://test.ggchan.dev';
const REQUEST_COUNT = 5;
const TIMEOUT_MS = 120000;

function makeRequest(targetUrl, method, headers, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const reqHeaders = { ...headers };
    delete reqHeaders.host;
    delete reqHeaders['content-length'];
    delete reqHeaders['accept-encoding'];
    reqHeaders['accept-encoding'] = 'identity';
    if (body) reqHeaders['content-length'] = Buffer.byteLength(body);

    const opts = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: method,
      headers: reqHeaders,
      timeout: TIMEOUT_MS,
    };

    const req = https.request(opts, (res) => {
      let chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const respBody = Buffer.concat(chunks).toString('utf-8');
        console.log(`    upstream response: ${res.statusCode}, body length: ${respBody.length}`);
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: respBody,
        });
      });
      res.on('error', (e) => {
        console.log(`    upstream response error: ${e.message}`);
        reject(e);
      });
    });

    req.on('error', (e) => {
      console.log(`    upstream request error: ${e.message}`);
      reject(e);
    });
    req.on('timeout', () => {
      console.log(`    upstream request timeout`);
      req.destroy();
      reject(new Error('Request timeout'));
    });

    if (body) req.write(body);
    req.end();
  });
}

function makeStreamingRequest(targetUrl, method, headers, body, onResponse, signal) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const opts = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: method,
      headers: {
        ...headers,
        host: url.hostname,
        'accept-encoding': 'identity',
      },
      timeout: TIMEOUT_MS,
    };

    const req = https.request(opts, (res) => {
      // 响应头一到达就回调，由调用方决定是否 pipe
      const chosen = onResponse(res, req);
      let totalSize = 0;

      if (!chosen) {
        // 没被选中，丢弃数据，等结束
        res.resume();
      } else {
        res.on('data', (chunk) => { totalSize += chunk.length; });
      }

      res.on('end', () => {
        resolve({ statusCode: res.statusCode, totalSize });
      });

      res.on('error', reject);
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    if (signal) {
      signal.addEventListener('abort', () => {
        req.destroy();
        reject(new Error('Aborted'));
      });
    }

    if (body) req.write(body);
    req.end();
  });
}

function countGeneratedWords(body) {
  try {
    const json = JSON.parse(body);
    if (json.choices && json.choices[0] && json.choices[0].message && json.choices[0].message.content) {
      return json.choices[0].message.content.length;
    }
    if (json.choices && json.choices[0] && json.choices[0].text) {
      return json.choices[0].text.length;
    }
    return body.length;
  } catch {
    return body.length;
  }
}

const server = http.createServer(async (req, res) => {
  const targetUrl = UPSTREAM + req.url;
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);

  let body = '';
  req.on('data', (chunk) => (body += chunk));
  req.on('end', async () => {
    const headers = { ...req.headers };
    delete headers.host;
    delete headers['content-length'];
    delete headers.connection;

    let isStream = false;
    try {
      if (body) {
        const parsed = JSON.parse(body);
        isStream = parsed.stream === true;
      }
    } catch {}

    if (isStream) {
      handleStreaming(req, res, targetUrl, headers, body);
    } else {
      handleNonStreaming(req, res, targetUrl, headers, body);
    }
  });
});

function handleStreaming(req, res, targetUrl, headers, body) {
  let winnerChosen = false;
  const abortControllers = [];
  let activeCount = 0;
  let errorCount = 0;

  for (let i = 0; i < REQUEST_COUNT; i++) {
    const controller = new AbortController();
    abortControllers.push(controller);
    activeCount++;

    makeStreamingRequest(
      targetUrl,
      req.method,
      headers,
      body,
      (upstreamRes, upstreamReq) => {
        if (winnerChosen) return false;
        winnerChosen = true;

        console.log(`  -> Stream winner: request #${i + 1}, status=${upstreamRes.statusCode}`);

        const streamHeaders = { ...upstreamRes.headers };
        delete streamHeaders.connection;
        streamHeaders['x-accel-buffering'] = 'no';
        streamHeaders['cache-control'] = 'no-cache, no-transform';
        streamHeaders['x-content-type-options'] = 'nosniff';
        res.writeHead(upstreamRes.statusCode, streamHeaders);

        upstreamRes.pipe(res);

        abortControllers.forEach((c, idx) => {
          if (idx !== i) {
            c.abort();
          }
        });
        return true;
      },
      controller.signal
    ).catch((err) => {
      errorCount++;
      console.log(`  -> Stream request #${i + 1} error: ${err.message}`);
    }).finally(() => {
      activeCount--;
      if (activeCount === 0 && !winnerChosen && !res.headersSent) {
        res.writeHead(502, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'All upstream requests failed', errors: errorCount }));
      }
    });
  }
}

async function handleNonStreaming(req, res, targetUrl, headers, body) {
  const promises = [];
  for (let i = 0; i < REQUEST_COUNT; i++) {
    promises.push(
      makeRequest(targetUrl, req.method, headers, body)
        .then((r) => ({ ...r, index: i }))
        .catch((err) => {
          console.log(`  -> Request #${i + 1} error: ${err.message}`);
          return null;
        })
    );
  }

  const results = await Promise.all(promises);
  const withResponse = results.filter((r) => r !== null);
  const successful = withResponse.filter((r) => r.statusCode < 400);

  if (withResponse.length === 0) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'All upstream requests failed (network error)' }));
    return;
  }

  // 如果全部是错误响应(>=400)，透传第一个错误响应给客户端
  if (successful.length === 0) {
    const firstErr = withResponse[0];
    console.log(`  -> All ${withResponse.length} requests returned errors, passthrough first: ${firstErr.statusCode}`);
    const errHeaders = { ...firstErr.headers };
    delete errHeaders.connection;
    delete errHeaders['transfer-encoding'];
    delete errHeaders['content-encoding'];
    errHeaders['content-length'] = Buffer.byteLength(firstErr.body);
    res.writeHead(firstErr.statusCode, errHeaders);
    res.end(firstErr.body);
    return;
  }

  let best = successful[0];
  let bestWords = countGeneratedWords(best.body);

  for (const r of successful) {
    const words = countGeneratedWords(r.body);
    console.log(`  -> Request #${r.index + 1}: status=${r.statusCode}, words=${words}`);
    if (words > bestWords) {
      best = r;
      bestWords = words;
    }
  }

  console.log(`  -> Winner: request #${best.index + 1}, words=${bestWords}`);

  const respHeaders = { ...best.headers };
  delete respHeaders.connection;
  delete respHeaders['transfer-encoding'];
  delete respHeaders['content-encoding'];
  respHeaders['content-length'] = Buffer.byteLength(best.body);
  res.writeHead(best.statusCode, respHeaders);
  res.end(best.body);
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`AI Proxy running on http://0.0.0.0:${PORT}`);
  console.log(`Upstream: ${UPSTREAM}`);
  console.log(`Concurrency: ${REQUEST_COUNT}`);
});
