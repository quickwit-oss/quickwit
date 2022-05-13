---
title: Ports configuration
sidebar_position: 5
---

When starting a quickwit search server, one important parameter that can be configured is
the `rest_listen_port` (defaults to :7280).

Internally, Quickwit will, in fact, use three sockets. The ports of these three sockets
cannot be configured independently at the moment.
The ports used are computed relative to the `rest_listen_port` port, as follows.


| Service                       | Port used                 | Protocol |  Default  |
|-------------------------------|---------------------------|----------|-----------|
| Http server with the rest api | `${rest_listen_port}`     |   TCP    | 7280      |
| Cluster membership            | `${rest_listen_port}`     |   UDP    | 7280      |
| GRPC service                  | `${rest_listen_port} + 1` |   TCP    | 7281      |

It is not possible for the moment to configure these ports independently.


In order to form a cluster, you will also need to define a `peer_seeds` parameter.
The following addresses are valid peer seed addresses:

| Type | Example without port | Example with port         |
|--------------|--------------|---------------------------|
| IPv4         | 172.1.0.12   | 172.1.0.12:7180           |
| IPv6         | 2001:0db8:85a3:0000:0000:8a2e:0370:7334  | [2001:0db8:85a3:0000:0000:8a2e:0370:7334:7180]:7280 |
| hostname     | node3        | node3:7180                |

If no port is specified in a peer node address, a Quickwit node will assume the peer is using the same port as itself.
