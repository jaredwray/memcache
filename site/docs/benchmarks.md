---
title: Benchmarks
description: Simple set/get throughput comparison against other Node.js Memcache clients.
order: 9
---

These are provided to show a simple benchmark against current libraries. This is not robust but it is something we update regularly to make sure we are keeping performant.

|             name             |  summary  |  ops/sec  |  time/op  |  margin  |  samples  |
|------------------------------|:---------:|----------:|----------:|:--------:|----------:|
|  memcache set/get (v1.4.0)   |    🥇     |       3K  |    350µs  |  ±0.19%  |      10K  |
|  memcached set/get (v2.2.2)  |   -2.9%   |       3K  |    361µs  |  ±0.16%  |      10K  |
|  memjs set/get (v1.3.2)      |   -12%    |       3K  |    398µs  |  ±0.17%  |      10K  |
