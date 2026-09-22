---
"nansen-cli": patch
---

EVM swaps and approvals refuse a quote whose worst-case gas cost (`maxFeePerGas` × gas limit) exceeds a 0.01 ETH safety cap, the EVM sibling of the priority-fee ceiling the Solana signer already enforces
