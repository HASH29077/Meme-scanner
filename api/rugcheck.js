// Proxies /api/rugcheck?address=<mint> to RugCheck's free, public
// report/summary endpoint. No API key is required for this endpoint —
// this proxy exists only to avoid the browser hitting CORS restrictions
// calling api.rugcheck.xyz directly, not to hide a secret.

export default async function handler(req, res) {
  const { address } = req.query;
  if (!address || Array.isArray(address)) {
    res.status(400).json({ error: 'Missing or invalid "address" query param.' });
    return;
  }

  const upstreamUrl = `https://api.rugcheck.xyz/v1/tokens/${address}/report/summary`;

  try {
    const upstreamRes = await fetch(upstreamUrl, { headers: { Accept: 'application/json' } });
    const body = await upstreamRes.text();
    res.status(upstreamRes.status);
    res.setHeader('Content-Type', upstreamRes.headers.get('content-type') || 'application/json');
    res.send(body);
  } catch (err) {
    res.status(502).json({ error: 'RugCheck proxy request failed', detail: err.message });
  }
}
