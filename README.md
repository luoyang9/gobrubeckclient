# gobrubeckclient
A brubeck (statsd replacement) client written in go

See https://github.com/github/brubeck for differences between statsd and brubeck

TL;DR: Brubeck supports a subset of metrics and does not support server side sampling.

This client implementation only supports counters and timers. In addition,
the client provides client-side sampling capabilities.
