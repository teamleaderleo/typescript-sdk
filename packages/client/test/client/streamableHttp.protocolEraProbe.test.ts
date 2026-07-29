import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { JSONRPCMessage } from '@modelcontextprotocol/core-internal';
import { PROTOCOL_VERSION_META_KEY } from '@modelcontextprotocol/core-internal';

import type { ReconnectionScheduler, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('StreamableHTTPClientTransport protocol-era reconnect probe', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 1,
        maxReconnectionDelay: 10,
        reconnectionDelayGrowFactor: 2,
        maxRetries: 2
    };

    async function waitForCallCount(mock: { mock: { calls: unknown[][] } }, count: number): Promise<void> {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            if (mock.mock.calls.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error(`Timed out waiting for ${count} calls; observed ${mock.mock.calls.length}`);
    }

    async function closeServer(server: ReturnType<typeof createServer>): Promise<void> {
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    }

    function modernRequest(id: string): JSONRPCMessage {
        return {
            jsonrpc: '2.0',
            id,
            method: 'tools/call',
            params: {
                name: 'slow',
                arguments: {},
                _meta: {
                    [PROTOCOL_VERSION_META_KEY]: '2026-07-28'
                }
            }
        } as JSONRPCMessage;
    }

    it('does not reconnect a compliant modern SSE response that closes without an event id', async () => {
        let getCount = 0;
        let observedProtocolVersion: string | undefined;
        let observedMethod: string | undefined;
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                observedProtocolVersion = req.headers['mcp-protocol-version'] as string | undefined;
                observedMethod = req.headers['mcp-method'] as string | undefined;
                req.resume();
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                // 2026-07-28 does not support resumable SSE. A retry field without
                // an event id must not create a resumable stream.
                res.end('retry: 7\ndata:\n\n');
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                res.writeHead(405).end();
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduler = vi.fn<ReconnectionScheduler>();
        const streamEnd = vi.fn();
        const onerror = vi.fn();
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            protocolVersion: '2026-07-28',
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onerror = onerror;

        try {
            await transport.start();
            await transport.send(modernRequest('modern-compliant'), { onRequestStreamEnd: streamEnd });
            await waitForCallCount(streamEnd, 1);

            expect(observedProtocolVersion).toBe('2026-07-28');
            expect(observedMethod).toBe('tools/call');
            expect(scheduler).not.toHaveBeenCalled();
            expect(getCount).toBe(0);
            expect(streamEnd).toHaveBeenCalledTimes(1);
            expect(onerror).not.toHaveBeenCalled();
        } finally {
            await transport.close();
            await closeServer(server);
        }
    });

    it('uses GET resumption for an explicit 2025-11-25 stream with an event id', async () => {
        let getCount = 0;
        let receivedLastEventId: string | undefined;
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                req.resume();
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                res.end('retry: 11\nid: legacy-0\ndata:\n\n');
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                const header = req.headers['last-event-id'];
                receivedLastEventId = Array.isArray(header) ? header[0] : header;
                res.writeHead(405).end();
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; delay: number; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, delay, attempt) => {
            scheduled.push({ reconnect, delay, attempt });
            return () => {};
        };
        const streamEnd = vi.fn();
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            protocolVersion: '2025-11-25',
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        try {
            await transport.start();
            await transport.send(
                { jsonrpc: '2.0', id: 'legacy-request', method: 'tools/call', params: { name: 'slow' } } as JSONRPCMessage,
                { onRequestStreamEnd: streamEnd }
            );
            const deadline = Date.now() + 5000;
            while (scheduled.length < 1 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
            expect(scheduled).toHaveLength(1);
            expect(scheduled[0]).toMatchObject({ delay: 11, attempt: 0 });

            scheduled[0]!.reconnect();
            await waitForCallCount(streamEnd, 1);

            expect(getCount).toBe(1);
            expect(receivedLastEventId).toBe('legacy-0');
            expect(streamEnd).toHaveBeenCalledTimes(1);
        } finally {
            await transport.close();
            await closeServer(server);
        }
    });

    it('still attempts forbidden GET resumption when a noncompliant modern server emits an event id', async () => {
        let getCount = 0;
        let receivedLastEventId: string | undefined;
        let getProtocolVersion: string | undefined;
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                req.resume();
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                // This response violates 2026-07-28, which does not support
                // Last-Event-ID resumption. The probe records client robustness.
                res.end('retry: 13\nid: modern-invalid-0\ndata:\n\n');
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                const header = req.headers['last-event-id'];
                receivedLastEventId = Array.isArray(header) ? header[0] : header;
                getProtocolVersion = req.headers['mcp-protocol-version'] as string | undefined;
                res.writeHead(405).end();
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; delay: number; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, delay, attempt) => {
            scheduled.push({ reconnect, delay, attempt });
            return () => {};
        };
        const streamEnd = vi.fn();
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            protocolVersion: '2026-07-28',
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        try {
            await transport.start();
            await transport.send(modernRequest('modern-invalid'), { onRequestStreamEnd: streamEnd });
            const deadline = Date.now() + 5000;
            while (scheduled.length < 1 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
            expect(scheduled).toHaveLength(1);
            expect(scheduled[0]).toMatchObject({ delay: 13, attempt: 0 });

            scheduled[0]!.reconnect();
            await waitForCallCount(streamEnd, 1);

            expect(getCount).toBe(1);
            expect(receivedLastEventId).toBe('modern-invalid-0');
            expect(getProtocolVersion).toBe('2026-07-28');
            expect(streamEnd).toHaveBeenCalledTimes(1);
        } finally {
            await transport.close();
            await closeServer(server);
        }
    });
});
