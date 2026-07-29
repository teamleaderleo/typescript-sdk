import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { JSONRPCMessage } from '@modelcontextprotocol/core-internal';

import { Client } from '../../src/client/client';
import type { ReconnectionScheduler, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('legacy Streamable HTTP reconnect chain after protocol timeout', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 1,
        maxReconnectionDelay: 10,
        reconnectionDelayGrowFactor: 2,
        maxRetries: 2
    };

    async function waitFor(predicate: () => boolean, description: string): Promise<void> {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            if (predicate()) return;
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error(`Timed out waiting for ${description}`);
    }

    async function closeServer(server: ReturnType<typeof createServer>): Promise<void> {
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    }

    function createLegacyServer(mode: 'keep-priming' | 'late-response') {
        let cancelledCount = 0;
        let resumedGetCount = 0;
        let toolRequestId: string | number | undefined;
        const lastEventIds: Array<string | undefined> = [];

        const server = createServer((req, res) => {
            if (req.method === 'GET') {
                const header = req.headers['last-event-id'];
                const lastEventId = Array.isArray(header) ? header[0] : header;
                if (lastEventId === undefined) {
                    // Client.connect() opens the optional standalone legacy GET
                    // after notifications/initialized. Keep it out of this probe.
                    res.writeHead(405).end();
                    return;
                }

                resumedGetCount += 1;
                lastEventIds.push(lastEventId);
                res.writeHead(200, { 'content-type': 'text/event-stream' });
                if (mode === 'late-response') {
                    res.end(
                        `id: late-final\ndata: ${JSON.stringify({
                            jsonrpc: '2.0',
                            id: toolRequestId,
                            result: { content: [{ type: 'text', text: 'late' }] }
                        })}\n\n`
                    );
                } else {
                    res.end(`retry: 1\nid: call-${resumedGetCount}\ndata:\n\n`);
                }
                return;
            }

            if (req.method !== 'POST') {
                res.writeHead(405).end();
                return;
            }

            let body = '';
            req.on('data', chunk => (body += String(chunk)));
            req.on('end', () => {
                const message = JSON.parse(body) as JSONRPCMessage;
                if ('method' in message && message.method === 'initialize') {
                    res.writeHead(200, { 'content-type': 'application/json' });
                    res.end(
                        JSON.stringify({
                            jsonrpc: '2.0',
                            id: message.id,
                            result: {
                                protocolVersion: '2025-11-25',
                                capabilities: { tools: {} },
                                serverInfo: { name: 'legacy-timeout-probe', version: '1.0.0' }
                            }
                        })
                    );
                    return;
                }
                if ('method' in message && message.method === 'notifications/initialized') {
                    res.writeHead(202).end();
                    return;
                }
                if ('method' in message && message.method === 'notifications/cancelled') {
                    cancelledCount += 1;
                    res.writeHead(202).end();
                    return;
                }
                if ('method' in message && message.method === 'tools/call') {
                    toolRequestId = message.id;
                    res.writeHead(200, { 'content-type': 'text/event-stream' });
                    res.end('retry: 1\nid: call-0\ndata:\n\n');
                    return;
                }
                res.writeHead(400).end();
            });
        });

        return {
            server,
            get cancelledCount() {
                return cancelledCount;
            },
            get resumedGetCount() {
                return resumedGetCount;
            },
            get lastEventIds() {
                return lastEventIds;
            }
        };
    }

    it('keeps a pending legacy reconnect chain alive after the caller request times out', async () => {
        const fixture = createLegacyServer('keep-priming');
        await new Promise<void>(resolve => fixture.server.listen(0, '127.0.0.1', resolve));
        const port = (fixture.server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, _delay, attempt) => {
            scheduled.push({ reconnect, attempt });
            return () => {};
        };
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        const client = new Client({ name: 'legacy-timeout-client', version: '1.0.0' });

        try {
            await client.connect(transport);
            const request = client.request(
                { method: 'tools/call', params: { name: 'slow', arguments: {} } },
                { timeout: 40 }
            );

            await waitFor(() => scheduled.length >= 1, 'initial reconnect schedule');
            await expect(request).rejects.toThrow('Request timed out');
            await waitFor(() => fixture.cancelledCount === 1, 'legacy cancellation notification');

            const pendingAfterTimeout = scheduled.shift();
            expect(pendingAfterTimeout).toBeDefined();
            pendingAfterTimeout!.reconnect();
            await waitFor(() => fixture.resumedGetCount === 1, 'resumed GET after timeout');
            await waitFor(() => scheduled.length >= 1, 'next reconnect schedule after timeout');

            expect(fixture.lastEventIds).toEqual(['call-0']);
            expect(scheduled[0]?.attempt).toBe(0);
        } finally {
            await client.close();
            await closeServer(fixture.server);
        }
    });

    it('continues real default-scheduler GET traffic after the caller has timed out', async () => {
        const fixture = createLegacyServer('keep-priming');
        await new Promise<void>(resolve => fixture.server.listen(0, '127.0.0.1', resolve));
        const port = (fixture.server.address() as AddressInfo).port;
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions
        });
        const client = new Client({ name: 'legacy-default-timer-client', version: '1.0.0' });

        try {
            await client.connect(transport);
            const request = client.request(
                { method: 'tools/call', params: { name: 'slow', arguments: {} } },
                { timeout: 60 }
            );

            await expect(request).rejects.toThrow('Request timed out');
            await waitFor(() => fixture.cancelledCount === 1, 'legacy cancellation notification');
            const getCountAfterTimeout = fixture.resumedGetCount;
            await waitFor(
                () => fixture.resumedGetCount > getCountAfterTimeout,
                'another real resumed GET after the request promise rejected'
            );

            expect(fixture.resumedGetCount).toBeGreaterThan(getCountAfterTimeout);
        } finally {
            await client.close();
            await closeServer(fixture.server);
        }
    });

    it('surfaces a late resumed response as an unknown message id after timeout cleanup', async () => {
        const fixture = createLegacyServer('late-response');
        await new Promise<void>(resolve => fixture.server.listen(0, '127.0.0.1', resolve));
        const port = (fixture.server.address() as AddressInfo).port;
        const scheduled: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler: ReconnectionScheduler = (reconnect, _delay, attempt) => {
            scheduled.push({ reconnect, attempt });
            return () => {};
        };
        const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        const client = new Client({ name: 'legacy-late-response-client', version: '1.0.0' });
        const errors: Error[] = [];
        client.onerror = error => errors.push(error);

        try {
            await client.connect(transport);
            const request = client.request(
                { method: 'tools/call', params: { name: 'slow', arguments: {} } },
                { timeout: 40 }
            );

            await waitFor(() => scheduled.length >= 1, 'initial reconnect schedule');
            await expect(request).rejects.toThrow('Request timed out');
            await waitFor(() => fixture.cancelledCount === 1, 'legacy cancellation notification');

            scheduled.shift()!.reconnect();
            await waitFor(() => fixture.resumedGetCount === 1, 'late response GET');
            await waitFor(
                () => errors.some(error => error.message.includes('Received a response for an unknown message ID')),
                'unknown message id diagnostic'
            );

            expect(fixture.lastEventIds).toEqual(['call-0']);
        } finally {
            await client.close();
            await closeServer(fixture.server);
        }
    });
});
