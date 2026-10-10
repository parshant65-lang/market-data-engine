# Market Data Engine

An autonomous, cloud-powered market intelligence harvester.

---

## ⚡ Architecture
- **Worker Environment:** GitHub Actions Public Cloud Runner (100% Free & Unlimited compute).
- **Target Channels:** Financial Times Institutional Feeds (Markets, FastFT, Global Economy, Cryptofinance, Alphaville, Technology, Companies, World).
- **Cadence:** Scheduled every 30 minutes (`*/30 * * * *`) + manual dispatch.
- **Vault Integration:** Streams deduplicated, sentiment-scored JSON records directly into an encrypted, private storage repository.
- **Privacy Assurance:** Public logs contain only telemetry statistics; article contents remain strictly confined to the private vault.
