// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation
import NIO

#if canImport(Network)
    import NIOTransportServices
#endif

/// The network transport the client uses to open TCP connections.
public enum NatsTransport: Sendable {
    /// BSD sockets via SwiftNIO's `ClientBootstrap`. The hostname is resolved by the client
    /// and the socket connects to an IP address, so only IP-based routing rules apply.
    case posixSockets

    /// Network.framework via `NIOTransportServices` (Apple platforms only).
    /// Connections are made by hostname, so the system applies per-domain VPN and
    /// split-tunnel rules the same way it does for `URLSession`.
    case networkFramework

    /// `.networkFramework` where available, otherwise `.posixSockets`.
    static var `default`: NatsTransport {
        #if canImport(Network)
            return .networkFramework
        #else
            return .posixSockets
        #endif
    }
}

/// Creates the event loop group and connection bootstraps for a transport.
internal struct TransportFactory: Sendable {
    let group: EventLoopGroup
    private let useNetworkFramework: Bool

    init(_ transport: NatsTransport) {
        #if canImport(Network)
            if transport == .networkFramework,
                #available(macOS 10.14, iOS 12.0, tvOS 12.0, watchOS 6.0, *)
            {
                self.group = NIOTSEventLoopGroup.singleton
                self.useNetworkFramework = true
                return
            }
        #endif
        self.group = MultiThreadedEventLoopGroup.singleton
        self.useNetworkFramework = false
    }

    /// A bootstrap with transport-specific options applied. The caller adds the
    /// channel initializer and connect timeout.
    func makeBootstrap() -> NIOClientTCPBootstrap {
        #if canImport(Network)
            if useNetworkFramework,
                #available(macOS 10.14, iOS 12.0, tvOS 12.0, watchOS 6.0, *),
                let tsGroup = group as? NIOTSEventLoopGroup
            {
                let bootstrap = NIOTSConnectionBootstrap(group: tsGroup)
                    // Fail with the underlying error (e.g. no route) instead of waiting
                    // silently for the network to become viable until the connect timeout.
                    .channelOption(NIOTSChannelOptions.waitForActivity, value: false)
                logger.debug("connecting via Network.framework (NIOTransportServices)")
                return NIOClientTCPBootstrap(bootstrap, tls: NIOInsecureNoTLS())
            }
        #endif
        logger.debug("connecting via POSIX sockets")
        let bootstrap = ClientBootstrap(group: group)
            .channelOption(
                ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
                value: 1
            )
        return NIOClientTCPBootstrap(bootstrap, tls: NIOInsecureNoTLS())
    }
}
