import type { StartSSEOptions, StreamableHTTPReconnectionOptions } from '../../src/client/streamableHttp';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

describe('StreamableHTTPClientTransport reconnect timer probe', () => {
    const reconnectionOptions: StreamableHTTPReconnectionOptions = {
        initialReconnectionDelay: 60_000,
        maxReconnectionDelay: 60_000,
        reconnectionDelayGrowFactor: 1,
        maxRetries: 3
    };

    function schedule(t: StreamableHTTPClientTransport, options: StartSSEOptions): void {
        (t as unknown as { _scheduleReconnection(options: StartSSEOptions, attempt?: number): void })._scheduleReconnection(options, 0);
    }

    it('leaves the older default reconnect timer pending after close()', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn();
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'), {
            fetch: fetchMock,
            reconnectionOptions
        });

        try {
            await transport.start();
            schedule(transport, { resumptionToken: 'stream-a' });
            schedule(transport, { resumptionToken: 'stream-b' });

            expect(vi.getTimerCount()).toBe(2);
            await transport.close();
            expect(vi.getTimerCount()).toBe(1);

            await vi.runAllTimersAsync();
            expect(fetchMock).not.toHaveBeenCalled();
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            vi.useRealTimers();
        }
    });
});
