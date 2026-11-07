# Insider Live

Always-on watcher for the insider teams found in the GMGN memecoin research (Solana, Robinhood Chain, BSC, Base, Ethereum).

- `scanner/scan.py` runs every ~10 minutes on GitHub Actions (free). It reads each watched wallet from public blockchain nodes, prices tokens with GeckoTerminal and checks new tokens past $500K.
- The dashboard is `docs/index.html`, served by GitHub Pages. It reads `docs/data/*.json`.
- `data/seed.txt` holds the teams and wallets from the research (one team per line).
- Run a scan now: Actions → scan → Run workflow.

Read-only research. Not financial advice.
