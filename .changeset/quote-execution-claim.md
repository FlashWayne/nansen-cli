---
"nansen-cli": patch
---

`trade execute` claims a quote before signing, so two concurrent runs of the same quote id cannot both broadcast a swap
