import type { ReconnectionScheduler, StartSSEOptions, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('StreamableHTTPClientTransport reconnect bookkeeping probe', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 10,
        maxReconnectionDelay: 10_000,
        reconnectionDelayGrowFactor: 2,
        maxRetries: 3
    };

    function schedule(t: StreamableHTTPClientTransport, options: StartSSEOptions = {}, attempt = 0): void {
        (t as unknown as { _scheduleReconnection(options: StartSSEOptions, attempt?: number): void })._scheduleReconnection(options, attempt);
    }

    function handleStream(
        t: StreamableHTTPClientTransport,
        body: ReadableStream<Uint8Array>,
        options: StartSSEOptions,
        isReconnectable = true
    ): void {
        (
            t as unknown as {
                _handleSseStream(body: ReadableStream<Uint8Array>, options: StartSSEOptions, isReconnectable: boolean): void;
            }
        )._handleSseStream(body, options, isReconnectable);
    }

    function sseBody(text: string): ReadableStream<Uint8Array> {
        const bytes = new TextEncoder().encode(text);
        return new ReadableStream<Uint8Array>({
            start(controller) {
                controller.enqueue(bytes);
                controller.close();
            }
        });
    }

    async function waitForCallCount(mock: { mock: { calls: unknown[][] } }, count: number): Promise<void> {
        for (let i = 0; i < 50; i++) {
            if (mock.mock.calls.length >= count) return;
            await new Promise(resolve => setTimeout(resolve, 0));
        }
        throw new Error(`Timed out waiting for ${count} calls; observed ${mock.mock.calls.length}`);
    }

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('documents that close() cancels only the latest of two pending reconnect schedules', async () => {
        const cancelA = vi.fn();
        const cancelB = vi.fn();
        const scheduler = vi.fn<ReconnectionScheduler>().mockReturnValueOnce(cancelA).mockReturnValueOnce(cancelB);
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        await transport.start();
        schedule(transport, { resumptionToken: 'stream-a' });
        schedule(transport, { resumptionToken: 'stream-b' });
        await transport.close();

        expect(scheduler).toHaveBeenCalledTimes(2);
        expect(cancelA).not.toHaveBeenCalled();
        expect(cancelB).toHaveBeenCalledTimes(1);
    });

    it('documents that an SSE retry field from one stream changes the delay used for another stream', async () => {
        const scheduler = vi.fn<ReconnectionScheduler>(() => () => {});
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        await transport.start();

        const streamA: StartSSEOptions = { resumptionToken: 'stream-a' };
        handleStream(transport, sseBody('retry: 50\nid: a\n\n'), streamA);
        await waitForCallCount(scheduler, 1);
        expect(scheduler.mock.calls[0]?.[1]).toBe(50);

        handleStream(transport, sseBody('retry: 5000\nid: b\n\n'), { resumptionToken: 'stream-b' });
        await waitForCallCount(scheduler, 2);
        expect(scheduler.mock.calls[1]?.[1]).toBe(5000);

        schedule(transport, streamA, 1);
        expect(scheduler.mock.calls[2]?.[1]).toBe(5000);

        await transport.close();
    });

    it('uses stream B retry advice for stream A after A reconnect fails', async () => {
        const pending: Array<{ reconnect: () => void; delay: number; attempt: number }> = [];
        const scheduler = vi.fn<ReconnectionScheduler>((reconnect, delay, attempt) => {
            pending.push({ reconnect, delay, attempt });
            return () => {};
        });
        const fetchMock = vi.fn(async () => {
            throw new TypeError('simulated network failure');
        });
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            fetch: fetchMock,
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onerror = vi.fn();

        await transport.start();
        handleStream(transport, sseBody('retry: 50\nid: a\n\n'), { resumptionToken: 'stream-a' });
        await waitForCallCount(scheduler, 1);
        handleStream(transport, sseBody('retry: 5000\nid: b\n\n'), { resumptionToken: 'stream-b' });
        await waitForCallCount(scheduler, 2);

        expect(pending[0]).toMatchObject({ delay: 50, attempt: 0 });
        expect(pending[1]).toMatchObject({ delay: 5000, attempt: 0 });

        pending[0]!.reconnect();
        await waitForCallCount(scheduler, 3);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(pending[2]).toMatchObject({ delay: 5000, attempt: 1 });

        await transport.close();
    });

    it('suppresses an older uncancelled reconnect callback after transport close', async () => {
        const pending: Array<{ reconnect: () => void; cancel: ReturnType<typeof vi.fn> }> = [];
        const scheduler = vi.fn<ReconnectionScheduler>(reconnect => {
            const cancel = vi.fn();
            pending.push({ reconnect, cancel });
            return cancel;
        });
        const fetchMock = vi.fn();
        const onerror = vi.fn();
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            fetch: fetchMock,
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });
        transport.onerror = onerror;

        await transport.start();
        schedule(transport, { resumptionToken: 'stream-a' });
        schedule(transport, { resumptionToken: 'stream-b' });
        await transport.close();

        expect(pending[0]!.cancel).not.toHaveBeenCalled();
        expect(pending[1]!.cancel).toHaveBeenCalledTimes(1);

        pending[0]!.reconnect();
        await new Promise(resolve => setTimeout(resolve, 0));

        expect(fetchMock).not.toHaveBeenCalled();
        expect(onerror).not.toHaveBeenCalled();
    });

    it('suppresses a pending reconnect after only that request is aborted', async () => {
        let reconnectA: (() => void) | undefined;
        const scheduler = vi.fn<ReconnectionScheduler>(reconnect => {
            reconnectA ??= reconnect;
            return () => {};
        });
        const fetchMock = vi.fn();
        const request = new AbortController();
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            fetch: fetchMock,
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        await transport.start();
        schedule(transport, { resumptionToken: 'stream-a', requestSignal: request.signal });
        request.abort();
        reconnectA?.();
        await new Promise(resolve => setTimeout(resolve, 0));

        expect(fetchMock).not.toHaveBeenCalled();
        await transport.close();
    });

    it('documents that successful reopen/drop cycles restart the retry attempt count at zero', async () => {
        const pending: Array<{ reconnect: () => void; attempt: number }> = [];
        const scheduler = vi.fn<ReconnectionScheduler>((reconnect, _delay, attempt) => {
            pending.push({ reconnect, attempt });
            return () => {};
        });
        let streamNumber = 0;
        const fetchMock = vi.fn(async () => {
            streamNumber += 1;
            return new Response(sseBody(`id: event-${streamNumber}\n\n`), {
                status: 200,
                headers: { 'content-type': 'text/event-stream' }
            });
        });
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            fetch: fetchMock,
            reconnectionOptions,
            reconnectionScheduler: scheduler
        });

        await transport.start();
        schedule(transport, { resumptionToken: 'event-0' });

        const observedAttempts: number[] = [];
        for (let i = 0; i < reconnectionOptions.maxRetries + 2; i++) {
            for (let spin = 0; spin < 50 && pending.length === 0; spin++) {
                await new Promise(resolve => setTimeout(resolve, 0));
            }
            const next = pending.shift();
            if (!next) throw new Error(`No reconnect callback available for cycle ${i}`);
            observedAttempts.push(next.attempt);
            next.reconnect();
            await new Promise(resolve => setTimeout(resolve, 0));
        }

        expect(observedAttempts).toEqual([0, 0, 0, 0, 0]);
        expect(fetchMock).toHaveBeenCalledTimes(5);

        await transport.close();
    });
});
