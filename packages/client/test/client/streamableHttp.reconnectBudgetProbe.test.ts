import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { JSONRPCMessage } from '@modelcontextprotocol/core-internal';

import type { ReconnectionScheduler, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('StreamableHTTPClientTransport reconnect budget probe', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 1,
        maxReconnectionDelay: 10,
        reconnectionDelayGrowFactor: 2,
        maxRetries: 2
    };

    async function waitForLength<T>(values: T[], count: number): Promise<void> {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            if (values.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error(`Timed out waiting for ${count} values; observed ${values.length}`);
    }

    async function waitForCallCount(mock: { mock: { calls: unknown[][] } }, count: number): Promise<void> {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            if (mock.mock.calls.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error(`Timed out waiting for ${count} calls; observed ${mock.mock.calls.length}`);
    }

    it('continues successful primed reopen/drop cycles beyond maxRetries at attempt zero', async () => {
        let getCount = 0;
        const receivedLastEventIds: Array<string | undefined> = [];
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                req.resume();
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                res.end('retry: 1\nid: event-0\ndata:\n\n');
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                const lastEventId = req.headers['last-event-id'];
                receivedLastEventIds.push(Array.isArray(lastEventId) ? lastEventId[0] : lastEventId);
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                res.end(`retry: 1\nid: event-${getCount}\ndata:\n\n`);
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, _delay, attempt) => {
            scheduled.push({ reconnect, attempt });
            return () => {};
        };
        const streamEnd = vi.fn();
        const onerror = vi.fn();
        const tokens: string[] = [];
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onerror = onerror;

        try {
            await transport.start();
            const request: JSONRPCMessage = { jsonrpc: '2.0', id: 'request-1', method: 'tools/call', params: { name: 'slow' } };
            await transport.send(request, {
                onresumptiontoken: token => tokens.push(token),
                onRequestStreamEnd: streamEnd
            });
            await waitForLength(scheduled, 1);

            const attempts: number[] = [];
            const cycles = reconnectionOptions.maxRetries + 4;
            for (let i = 0; i < cycles; i++) {
                const next = scheduled.shift();
                if (!next) throw new Error(`Missing reconnect callback for cycle ${i}`);
                attempts.push(next.attempt);
                next.reconnect();
                await waitForLength(scheduled, 1);
            }

            expect(attempts).toEqual(Array(cycles).fill(0));
            expect(getCount).toBe(cycles);
            expect(receivedLastEventIds).toEqual(['event-0', 'event-1', 'event-2', 'event-3', 'event-4', 'event-5']);
            expect(tokens).toEqual(['event-0', 'event-1', 'event-2', 'event-3', 'event-4', 'event-5', 'event-6']);
            expect(streamEnd).not.toHaveBeenCalled();
            expect(onerror).not.toHaveBeenCalled();
        } finally {
            await transport.close();
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
        }
    });

    it('exhausts consecutive failed GET opens, ends that request stream, and keeps the transport usable', async () => {
        let postCount = 0;
        let getCount = 0;
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                let body = '';
                req.on('data', chunk => (body += String(chunk)));
                req.on('end', () => {
                    postCount += 1;
                    const message = JSON.parse(body) as { id?: string | number };
                    if (postCount === 1) {
                        res.writeHead(200, { 'content-type': 'text/event-stream' });
                        res.end('retry: 1\nid: failed-0\ndata:\n\n');
                    } else {
                        res.writeHead(200, { 'content-type': 'application/json' });
                        res.end(JSON.stringify({ jsonrpc: '2.0', id: message.id, result: { recovered: true } }));
                    }
                });
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                res.writeHead(503, { 'content-type': 'text/plain' });
                res.end('still unavailable');
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, _delay, attempt) => {
            scheduled.push({ reconnect, attempt });
            return () => {};
        };
        const streamEnd = vi.fn();
        const errors: Error[] = [];
        const messages: JSONRPCMessage[] = [];
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onerror = error => errors.push(error);
        transport.onmessage = message => messages.push(message);

        try {
            await transport.start();
            await transport.send(
                { jsonrpc: '2.0', id: 'request-fails', method: 'tools/call', params: { name: 'slow' } },
                { onRequestStreamEnd: streamEnd }
            );
            await waitForLength(scheduled, 1);

            const first = scheduled.shift()!;
            expect(first.attempt).toBe(0);
            first.reconnect();
            await waitForLength(scheduled, 1);

            const second = scheduled.shift()!;
            expect(second.attempt).toBe(1);
            second.reconnect();
            await waitForCallCount(streamEnd, 1);

            expect(getCount).toBe(2);
            expect(streamEnd).toHaveBeenCalledTimes(1);
            expect(errors.some(error => error.message.includes('Maximum reconnection attempts (2) exceeded'))).toBe(true);

            await transport.send({ jsonrpc: '2.0', id: 'request-after', method: 'tools/call', params: { name: 'after' } });
            await waitForLength(messages, 1);
            expect(messages).toContainEqual({ jsonrpc: '2.0', id: 'request-after', result: { recovered: true } });
            expect(postCount).toBe(2);
        } finally {
            await transport.close();
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
        }
    });

    it('stops reconnecting after a resumed stream delivers the JSON-RPC response', async () => {
        let getCount = 0;
        const server = createServer((req, res) => {
            if (req.method === 'POST') {
                req.resume();
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                res.end('retry: 1\nid: result-0\ndata:\n\n');
                return;
            }
            if (req.method === 'GET') {
                getCount += 1;
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                res.end(
                    `id: result-1\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: 'request-result', result: { ok: true } })}\n\n`
                );
                return;
            }
            res.writeHead(405).end();
        });

        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = (server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, _delay, attempt) => {
            scheduled.push({ reconnect, attempt });
            return () => {};
        };
        const streamEnd = vi.fn();
        const messages: JSONRPCMessage[] = [];
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onmessage = message => messages.push(message);

        try {
            await transport.start();
            await transport.send(
                { jsonrpc: '2.0', id: 'request-result', method: 'tools/call', params: { name: 'slow' } },
                { onRequestStreamEnd: streamEnd }
            );
            await waitForLength(scheduled, 1);
            scheduled.shift()!.reconnect();
            await waitForLength(messages, 1);
            await waitForCallCount(streamEnd, 1);

            expect(getCount).toBe(1);
            expect(messages).toEqual([{ jsonrpc: '2.0', id: 'request-result', result: { ok: true } }]);
            expect(streamEnd).toHaveBeenCalledTimes(1);
            expect(scheduled).toHaveLength(0);
        } finally {
            await transport.close();
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
        }
    });
});
