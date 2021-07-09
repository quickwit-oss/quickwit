---
title: Telemetry
position: 5
---

Quickiwt Inc. collects anonymous data regarding general usage to help us driving our development. Privacy and transparency are at the heart of Quickwit values and we do collect the minimal useful data and don't use any third party tool for the collection. 

## Disabling data collection

Data collection are opt-out. To disable them, just set the environment variable `DISABLE_QUICKWIT_TELEMETRY` to whatever vlue
```
export DISABLE_QUICKWIT_TELEMETRY=1
```


## Which data are collected?

We collect the minimum amount of information to respect privacy. Here are the data collected:
- type of events among create, index, delete and serve events
- client information:
  - session uuid: uuid generated on the fly
  - quickwit version
  - os (linux, macos, freebsd, android...)
  - architecture of the CPU
  - md5 hash of host and username 
  - a boolean to know if `KUBERNETES_SERVICE_HOST` is set.


## No third party

We did not want to add an untrusted third party tool in the collection so we decided to implement and host ourselves the collecting server telemetry.quickwit.io.
