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
        for (let i = 0; i < 100; i++) {
            if (values.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 0));
        }
        throw new Error(`Timed out waiting for ${count} values; observed ${values.length}`);
    }

    it('applies stream B retry advice to stream A after a real resumed GET fails', async () => {
        let postCount = 0;
        const resumedEventIds: Array<string | undefined> = [];
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                req.resume();
                postCount += 1;
                const stream = postCount === 1 ? { id: 'a-1', retry: 50 } : { id: 'b-1', retry: 5000 };
                res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
                res.end(`retry: ${stream.retry}\nid: ${stream.id}\n\n`);
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

            expect(scheduled[0]).toMatchObject({ delay: 50, attempt: 0 });
            expect(scheduled[1]).toMatchObject({ delay: 5000, attempt: 0 });

            scheduled[0]!.reconnect();
            await waitForLength(scheduled, 3);

            expect(resumedEventIds).toEqual(['a-1']);
            expect(scheduled[2]).toMatchObject({ delay: 5000, attempt: 1 });
            expect(onerror).toHaveBeenCalled();
        } finally {
            await transport.close();
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
        }
    });
});
