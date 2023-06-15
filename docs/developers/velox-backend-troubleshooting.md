---
layout: page
title: Troubleshooting
nav_order: 10
parent: Developer Overview
---
## Troubleshooting

### Fatal error after native exception is thrown

We depend on checking exceptions thrown from native code to validate whether a spark plan can
be really offloaded to native engine. But if `libunwind-dev` is installed, native exception will not be
caught and will interrupt the program. So far, we observed this fatal error can happen only on Ubuntu 20.04.
Please remove `libunwind-dev` to address this issue.

`sudo apt-get purge --auto-remove libunwind-dev`
