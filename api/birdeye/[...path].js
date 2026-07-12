// Proxies every /api/birdeye/<...> request to https://public-api.birdeye.so/<...>
// The real key lives only in the Vercel env var BIRDEYE_API_KEY — it never
// reaches the browser. The frontend calls BIRDEYE_BASE = '/api/birdeye'.

export default async function handler(req, res) {
  const apiKey = process.env.BIRDEYE_API_KEY;
  if (!apiKey) {
    res.status(500).json({ error: 'BIRDEYE_API_KEY is not set in the deployment environment.' });
    return;
  }

  const { path = [] } = req.query;
  const upstreamPath = Array.isArray(path) ? path.join('/') : String(path);

  // Rebuild the query string, dropping the catch-all `path` param itself.
  const qs = new URLSearchParams(req.query);
  qs.delete('path');
  const queryString = qs.toString();

  const upstreamUrl =
    `https://public-api.birdeye.so/${upstreamPath}${queryString ? '?' + queryString : ''}`;

  const upstreamHeaders = {
    'X-API-KEY': apiKey,
    'Authorization': 'Bearer ' + apiKey,
    'x-chain': req.headers['x-chain'] || 'solana',
    'Accept': 'application/json',
  };

  try {
    const upstreamRes = await fetch(upstreamUrl, { method: 'GET', headers: upstreamHeaders });
    const body = await upstreamRes.text();
    res.status(upstreamRes.status);
    res.setHeader('Content-Type', upstreamRes.headers.get('content-type') || 'application/json');
    res.send(body);
  } catch (err) {
    res.status(502).json({ error: 'Birdeye proxy request failed', detail: err.message });
  }
}
