---
"nansen-cli": patch
---

x402 Solana payments refuse an option whose `feePayer` is the paying wallet, instead of building a transaction that fails at broadcast. The payment loop now logs why it skipped an option and moves on to the next one.
