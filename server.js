const https = require('https');
const http = require('http');

const UPSTREAM = 'https://test.ggchan.dev';
const REQUEST_COUNT = 5;
const TIMEOUT_MS = 120000;

function makeRequest(targetUrl, method, headers, body) {
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
      },
      timeout: TIMEOUT_MS,
    };

    const req = https.request(opts, (res) => {
      let chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks).toString('utf-8'),
        });
      });
      res.on('error', reject);
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    if (body) req.write(body);
    req.end();
  });
}

function makeStreamingRequest(targetUrl, method, headers, body, onFirstByte, signal) {
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
      },
      timeout: TIMEOUT_MS,
    };

    const req = https.request(opts, (res) => {
      let firstByteReceived = false;
      let totalSize = 0;

      res.on('data', (chunk) => {
        if (!firstByteReceived) {
          firstByteReceived = true;
          onFirstByte(res, req);
        }
        totalSize += chunk.length;
      });

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
        if (winnerChosen) return;
        winnerChosen = true;

        console.log(`  -> Stream winner: request #${i + 1}, status=${upstreamRes.statusCode}`);

        res.writeHead(upstreamRes.statusCode, {
          ...upstreamRes.headers,
          connection: 'close',
        });

        upstreamRes.pipe(res);

        abortControllers.forEach((c, idx) => {
          if (idx !== i) {
            c.abort();
          }
        });
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
  const successful = results.filter((r) => r && r.statusCode < 400);

  if (successful.length === 0) {
    const firstError = results.find((r) => r === null);
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'All upstream requests failed or returned errors' }));
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

  res.writeHead(best.statusCode, {
    ...best.headers,
    connection: 'close',
  });
  res.end(best.body);
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`AI Proxy running on http://0.0.0.0:${PORT}`);
  console.log(`Upstream: ${UPSTREAM}`);
  console.log(`Concurrency: ${REQUEST_COUNT}`);
});
