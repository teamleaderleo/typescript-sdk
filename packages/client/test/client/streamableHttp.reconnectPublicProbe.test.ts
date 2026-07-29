import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { JSONRPCMessage } from '@modelcontextprotocol/core-internal';

import type { ReconnectionScheduler, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('StreamableHTTPClientTransport public reconnect probe', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 10,
        maxReconnectionDelay: 10_000,
        reconnectionDelayGrowFactor: 2,
        maxRetries: 3
    };

    async function waitForLength<T>(values: T[], count: number): Promise<void> {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            if (values.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error(`Timed out waiting for ${count} values; observed ${values.length}`);
    }

    it.each([
        { streamARetry: 50, streamBRetry: 5000 },
        { streamARetry: 5000, streamBRetry: 50 }
    ])(
        'uses stream B retry=$streamBRetry for stream A after A retry=$streamARetry and a real resumed GET failure',
        async ({ streamARetry, streamBRetry }) => {
            let postCount = 0;
            const resumedEventIds: Array<string | undefined> = [];
            const server = createServer((req, res) => {
                if (req.method === 'POST') {
                    req.on('error', () => {});
                    req.on('data', () => {});
                    req.on('end', () => {
                        postCount += 1;
                        const stream =
                            postCount === 1 ? { id: 'a-1', retry: streamARetry } : { id: 'b-1', retry: streamBRetry };
                        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
                        res.end(`retry: ${stream.retry}\nid: ${stream.id}\ndata:\n\n`);
                    });
                    return;
                }

                if (req.method === 'GET') {
                    const lastEventId = req.headers['last-event-id'];
                    resumedEventIds.push(Array.isArray(lastEventId) ? lastEventId[0] : lastEventId);
                    res.writeHead(503, { 'content-type': 'text/plain' });
                    res.end('simulated reopen failure');
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
            const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
                reconnectionOptions,
                reconnectionScheduler: scheduler
            });
            const onerror = vi.fn();
            transport.onerror = onerror;

            try {
                await transport.start();
                const requestA: JSONRPCMessage = { jsonrpc: '2.0', id: 'request-a', method: 'tools/call', params: { name: 'a' } };
                const requestB: JSONRPCMessage = { jsonrpc: '2.0', id: 'request-b', method: 'tools/call', params: { name: 'b' } };

                await transport.send(requestA);
                await transport.send(requestB);
                await waitForLength(scheduled, 2);

                expect(scheduled[0]).toMatchObject({ delay: streamARetry, attempt: 0 });
                expect(scheduled[1]).toMatchObject({ delay: streamBRetry, attempt: 0 });

                scheduled[0]!.reconnect();
                await waitForLength(scheduled, 3);

                expect(resumedEventIds).toEqual(['a-1']);
                expect(scheduled[2]).toMatchObject({ delay: streamBRetry, attempt: 1 });
                expect(onerror).toHaveBeenCalled();
            } finally {
                await transport.close();
                await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
            }
        }
    );
});
