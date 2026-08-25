# Cloudflare Tunnel Runbook

Use Cloudflare Tunnel to share a scheduled demo without opening router ports.

1. Install `cloudflared` using Cloudflare's official documentation.
2. Run `cloudflared tunnel login` and `cloudflared tunnel create thesis-demo`.
3. Copy `cloudflared/config.example.yml` to `cloudflared/config.yml`, then insert the tunnel UUID and credentials-file path.
4. Run `cloudflared tunnel route dns thesis-demo airflow-demo.your-domain.example`.
5. In Cloudflare Zero Trust, protect the hostname with an Access Allow policy for only your email and invited evaluators.
6. Start the local Docker ecosystem, then run `cloudflared tunnel run thesis-demo` only for the demo.

Never tunnel ports `5432`, `5433`, `8123`, or `9000`. Keep credentials JSON files and `.env` files out of Git.
