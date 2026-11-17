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
import NIOConcurrencyHelpers

#if canImport(Network)
    import Network
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

    init(_ transport: NatsTransport) {
        #if canImport(Network)
            if transport == .networkFramework {
                self.group = NIOTSEventLoopGroup.singleton
                return
            }
        #endif
        self.group = MultiThreadedEventLoopGroup.singleton
    }

    /// Whether connections go through Network.framework. Derived from `group` so the two
    /// can never disagree.
    var usesNetworkFramework: Bool {
        #if canImport(Network)
            return group is NIOTSEventLoopGroup
        #else
            return false
        #endif
    }

    /// A bootstrap with transport-specific options applied. The caller adds the
    /// channel initializer and connect timeout.
    func makeBootstrap() -> NIOClientTCPBootstrap {
        #if canImport(Network)
            if usesNetworkFramework {
                logger.debug("connecting via Network.framework (NIOTransportServices)")
                // waitForActivity stays at its default (true): Network.framework may need a
                // moment for the path to become viable, e.g. while an on-demand VPN comes up.
                // ConnectivityWaitHandler fails fast on errors that waiting cannot fix, and
                // the connect timeout bounds the wait otherwise.
                let bootstrap = NIOTSConnectionBootstrap(group: group)
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

    /// Adds transport-specific handlers to a new channel. Must be called from the channel
    /// initializer, on the channel's event loop, before any other handler is added.
    func prepare(_ channel: Channel, for attempt: ConnectAttempt) throws {
        #if canImport(Network)
            if usesNetworkFramework {
                try channel.pipeline.syncOperations.addHandler(
                    ConnectivityWaitHandler(attempt: attempt))
            }
        #endif
    }
}

/// State for a single connection attempt, shared between the channel pipeline and the
/// code awaiting the connect.
internal final class ConnectAttempt: Sendable {
    /// The last reason Network.framework gave for not being able to connect yet.
    private let waitingError = NIOLockedValueBox<Error?>(nil)

    func recordWaitingError(_ error: Error) {
        waitingError.withLockedValue { $0 = error }
    }

    /// The error to report for a failed connect.
    ///
    /// When Network.framework was waiting for connectivity, the connect ends with a generic
    /// close or timeout error. The recorded waiting error explains the actual cause.
    func failure(for error: Error) -> Error {
        waitingError.withLockedValue { $0 } ?? error
    }
}

/// Maps transport-specific connect errors onto ``NatsError/ConnectError``.
/// Returns nil for errors the caller should classify itself.
internal func transportConnectError(_ error: Error) -> NatsError.ConnectError? {
    #if canImport(Network)
        if let error = error as? NWError {
            if case .dns = error {
                return .dns(error)
            }
            return .io(error)
        }
    #endif
    return nil
}

#if canImport(Network)
    /// Watches Network.framework's waiting-for-connectivity events during connect.
    ///
    /// The error is recorded so it can be reported if the connect fails. DNS failures and
    /// refused connections close the channel at once, matching POSIX sockets, so reconnect and
    /// server failover are not slowed down by the connect timeout. Other errors, such as no
    /// viable network path, keep waiting until the connect timeout. The handler removes
    /// itself once the connection is active.
    final class ConnectivityWaitHandler: ChannelInboundHandler, RemovableChannelHandler {
        typealias InboundIn = ByteBuffer

        private let attempt: ConnectAttempt

        init(attempt: ConnectAttempt) {
            self.attempt = attempt
        }

        func channelActive(context: ChannelHandlerContext) {
            context.fireChannelActive()
            context.pipeline.removeHandler(context: context, promise: nil)
        }

        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            if let waiting = event as? NIOTSNetworkEvents.WaitingForConnectivity {
                let error = waiting.transientError
                attempt.recordWaitingError(error)
                if Self.waitingCannotHelp(error) {
                    logger.debug("connect failed: \(error)")
                    context.close(promise: nil)
                    return
                } else {
                    logger.debug("waiting for connectivity: \(error)")
                }
            }
            context.fireUserInboundEventTriggered(event)
        }

        private static func waitingCannotHelp(_ error: NWError) -> Bool {
            switch error {
            case .dns:
                return true
            case .posix(let code):
                return code == .ECONNREFUSED
            default:
                return false
            }
        }
    }
#endif
