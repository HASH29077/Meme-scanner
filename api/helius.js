// Proxies /api/helius (POST, JSON-RPC body) to the real Helius RPC endpoint.
// The real key lives only in the Vercel env var HELIUS_API_KEY.

export default async function handler(req, res) {
  const apiKey = process.env.HELIUS_API_KEY;
  if (!apiKey) {
    res.status(500).json({ error: 'HELIUS_API_KEY is not set in the deployment environment.' });
    return;
  }

  if (req.method !== 'POST') {
    res.status(405).json({ error: 'Only POST is supported.' });
    return;
  }

  const upstreamUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;

  try {
    const upstreamRes = await fetch(upstreamUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    const body = await upstreamRes.text();
    res.status(upstreamRes.status);
    res.setHeader('Content-Type', upstreamRes.headers.get('content-type') || 'application/json');
    res.send(body);
  } catch (err) {
    res.status(502).json({ error: 'Helius proxy request failed', detail: err.message });
  }
}
