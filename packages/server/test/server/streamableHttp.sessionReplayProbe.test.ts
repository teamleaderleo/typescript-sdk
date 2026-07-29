import { randomUUID } from 'node:crypto';

import type { CallToolResult, JSONRPCMessage } from '@modelcontextprotocol/core-internal';
import * as z from 'zod/v4';

import { McpServer } from '../../src/server/mcp';
import type { EventId, EventStore, StreamId } from '../../src/server/streamableHttp';
import { WebStandardStreamableHTTPServerTransport } from '../../src/server/streamableHttp';

describe('Streamable HTTP session replay probe', () => {
    it('replays a stored final result more than once while executing the tool once', async () => {
        const stored = new Map<EventId, { streamId: StreamId; message: JSONRPCMessage }>();
        let eventSequence = 0;
        const eventStore: EventStore = {
            async storeEvent(streamId, message) {
                const eventId = `${streamId}:event-${++eventSequence}`;
                stored.set(eventId, { streamId, message });
                return eventId;
            },
            async getStreamIdForEventId(eventId) {
                return stored.get(eventId)?.streamId;
            },
            async replayEventsAfter(lastEventId, { send }) {
                const last = stored.get(lastEventId);
                if (!last) throw new Error(`Unknown event ${lastEventId}`);
                let afterLast = false;
                for (const [eventId, event] of stored) {
                    if (eventId === lastEventId) {
                        afterLast = true;
                        continue;
                    }
                    if (afterLast && event.streamId === last.streamId) {
                        await send(eventId, event.message);
                    }
                }
                return last.streamId;
            }
        };

        let releaseHandler!: () => void;
        const handlerGate = new Promise<void>(resolve => {
            releaseHandler = resolve;
        });
        let executions = 0;
        const server = new McpServer({ name: 'session-replay-probe', version: '0.0.1' });
        server.registerTool(
            'durable_effect',
            { inputSchema: z.object({ operationId: z.string() }) },
            async ({ operationId }, ctx): Promise<CallToolResult> => {
                executions += 1;
                ctx.http?.closeSSE?.();
                await handlerGate;
                return {
                    content: [{ type: 'text', text: JSON.stringify({ operationId, executions }) }]
                };
            }
        );

        const transport = new WebStandardStreamableHTTPServerTransport({
            sessionIdGenerator: () => randomUUID(),
            eventStore
        });
        await server.connect(transport);

        try {
            const initialized = await transport.handleRequest(
                request('POST', {
                    jsonrpc: '2.0',
                    id: 'initialize-1',
                    method: 'initialize',
                    params: {
                        protocolVersion: '2025-11-25',
                        capabilities: {},
                        clientInfo: { name: 'session-replay-probe', version: '0.0.1' }
                    }
                })
            );
            const sessionId = initialized.headers.get('mcp-session-id');
            expect(sessionId).toBeTruthy();
            await initialized.text();
            stored.clear();

            const original = await transport.handleRequest(
                request(
                    'POST',
                    {
                        jsonrpc: '2.0',
                        id: 'call-1',
                        method: 'tools/call',
                        params: { name: 'durable_effect', arguments: { operationId: 'operation-1' } }
                    },
                    { sessionId: sessionId! }
                )
            );
            const primingText = await original.text();
            const primingEventId = /id:\s*(\S+)/.exec(primingText)?.[1];
            expect(primingEventId).toBeTruthy();
            expect(executions).toBe(1);

            releaseHandler();
            await waitFor(() =>
                [...stored.values()].some(message =>
                    'id' in message.message && message.message.id === 'call-1' && 'result' in message.message
                )
            );

            const firstReplay = await transport.handleRequest(
                request('GET', undefined, { sessionId: sessionId!, lastEventId: primingEventId! })
            );
            expect(firstReplay.status).toBe(200);
            expect(parseToolPayload(await firstReplay.text())).toEqual({ operationId: 'operation-1', executions: 1 });
            expect(executions).toBe(1);

            const secondReplay = await transport.handleRequest(
                request('GET', undefined, { sessionId: sessionId!, lastEventId: primingEventId! })
            );
            expect(secondReplay.status).toBe(200);
            expect(parseToolPayload(await secondReplay.text())).toEqual({ operationId: 'operation-1', executions: 1 });
            expect(executions).toBe(1);
        } finally {
            await transport.close();
            await server.close();
        }
    });
});

function request(
    method: 'POST' | 'GET',
    body?: JSONRPCMessage,
    options?: { sessionId?: string; lastEventId?: string }
): Request {
    const headers = new Headers();
    headers.set('accept', method === 'GET' ? 'text/event-stream' : 'application/json, text/event-stream');
    if (body) headers.set('content-type', 'application/json');
    if (options?.sessionId) {
        headers.set('mcp-session-id', options.sessionId);
        headers.set('mcp-protocol-version', '2025-11-25');
    }
    if (options?.lastEventId) headers.set('last-event-id', options.lastEventId);
    return new Request('http://localhost/mcp', {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined
    });
}

function parseToolPayload(text: string): { operationId: string; executions: number } {
    const data = text
        .split('\n')
        .find(line => line.startsWith('data:'))
        ?.slice(5)
        .trim();
    if (!data) throw new Error('Replay did not contain SSE data');
    const message = JSON.parse(data) as {
        result?: { content?: Array<{ type?: unknown; text?: unknown }> };
    };
    const first = message.result?.content?.[0];
    if (first?.type !== 'text' || typeof first.text !== 'string') {
        throw new Error('Replay did not contain MCP text content');
    }
    return JSON.parse(first.text) as { operationId: string; executions: number };
}

async function waitFor(predicate: () => boolean): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
        if (predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error('Timed out waiting for stored final response');
}
