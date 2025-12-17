const std = @import("std");
const testing = std.testing;

const model = @import("../model.zig");
const provider = @import("provider.zig");

const fallback_pricing = [_]provider.FallbackPricingEntry{};

const ProviderExports = provider.makeProvider(.{
    .name = "amp",
    .sessions_dir_suffix = "/.local/share/amp/threads",
    .session_file_ext = ".json",
    .legacy_fallback_model = null,
    .fallback_pricing = fallback_pricing[0..],
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseSessionFile,
});

pub const collect = ProviderExports.collect;
pub const streamEvents = ProviderExports.streamEvents;
pub const loadPricingData = ProviderExports.loadPricingData;
pub const EventConsumer = ProviderExports.EventConsumer;
pub const sessionsPath = ProviderExports.sessionsPath;

const UsageEntry = struct {
    model: ?[]const u8 = null,
    usage: model.TokenUsage = .{},
};

const LedgerTokens = struct {
    input: u64 = 0,
    output: u64 = 0,
};

fn parseSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*provider.MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    _ = deduper;
    const MessageUsage = struct {
        model: ?[]const u8 = null,
        inputTokens: ?u64 = null,
        cacheCreationInputTokens: ?u64 = null,
        cacheReadInputTokens: ?u64 = null,
        outputTokens: ?u64 = null,
        reasoningOutputTokens: ?u64 = null,
        totalInputTokens: ?u64 = null,
        totalTokens: ?u64 = null,
    };

    const Message = struct {
        messageId: ?u64 = null,
        usage: ?MessageUsage = null,
    };

    const LedgerTokensJson = struct {
        input: ?u64 = null,
        output: ?u64 = null,
    };

    const LedgerEvent = struct {
        timestamp: ?[]const u8 = null,
        model: ?[]const u8 = null,
        tokens: ?LedgerTokensJson = null,
        fromMessageId: ?u64 = null,
        toMessageId: ?u64 = null,
    };

    const UsageLedger = struct {
        events: []LedgerEvent = &.{},
    };

    const SessionDoc = struct {
        messages: []Message = &.{},
        usageLedger: ?UsageLedger = null,
    };

    const Handler = struct {
        allocator: std.mem.Allocator,
        ctx: *const provider.ParseContext,
        session_id: []const u8,
        timezone_offset_minutes: i32,
        sink: provider.EventSink,

        fn run(self: *@This(), scratch: std.mem.Allocator, reader: *std.json.Reader) !void {
            var parsed = try std.json.parseFromTokenSource(SessionDoc, scratch, reader, .{
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var usage_by_message = std.AutoHashMap(u64, UsageEntry).init(scratch);
            defer usage_by_message.deinit();

            for (parsed.value.messages) |msg| {
                const message_id = msg.messageId orelse continue;
                const usage_json = msg.usage orelse continue;

                var entry = UsageEntry{};
                entry.model = usage_json.model;

                entry.usage.input_tokens = usage_json.inputTokens orelse 0;
                entry.usage.cache_creation_input_tokens = usage_json.cacheCreationInputTokens orelse 0;
                entry.usage.cached_input_tokens = usage_json.cacheReadInputTokens orelse 0;
                entry.usage.output_tokens = usage_json.outputTokens orelse 0;
                entry.usage.reasoning_output_tokens = usage_json.reasoningOutputTokens orelse 0;

                const total_input = usage_json.totalInputTokens orelse
                    (entry.usage.input_tokens +| entry.usage.cache_creation_input_tokens +| entry.usage.cached_input_tokens);
                entry.usage.total_tokens = usage_json.totalTokens orelse
                    (total_input +| entry.usage.output_tokens +| entry.usage.reasoning_output_tokens);

                try usage_by_message.put(message_id, entry);
            }

            const ledger = parsed.value.usageLedger orelse return;
            for (ledger.events) |event| {
                const target_id = event.toMessageId orelse event.fromMessageId orelse continue;
                const ts = event.timestamp orelse continue;

                const timestamp_info = (try provider.timestampFromSlice(
                    self.allocator,
                    ts,
                    self.timezone_offset_minutes,
                )) orelse continue;

                var timestamp_slot: ?provider.TimestampInfo = timestamp_info;
                defer if (timestamp_slot) |info| self.allocator.free(info.text);

                var ledger_tokens = LedgerTokens{};
                if (event.tokens) |tok| {
                    ledger_tokens.input = tok.input orelse 0;
                    ledger_tokens.output = tok.output orelse 0;
                }

                const ledger_model = event.model;

                var usage = model.TokenUsage{
                    .input_tokens = ledger_tokens.input,
                    .output_tokens = ledger_tokens.output,
                    .total_tokens = ledger_tokens.input + ledger_tokens.output,
                };
                var raw_model: ?[]const u8 = ledger_model;

                if (usage_by_message.get(target_id)) |msg_usage| {
                    usage = msg_usage.usage;
                    if (msg_usage.model) |m| raw_model = m;
                }

                const model_slice = raw_model orelse continue;

                var model_state = provider.ModelState{};
                provider.emitUsageEventWithTimestamp(
                    self.ctx,
                    self.allocator,
                    &model_state,
                    self.sink,
                    self.session_id,
                    &timestamp_slot,
                    usage,
                    model_slice,
                ) catch continue;
            }
        }
    };

    var handler = Handler{
        .allocator = allocator,
        .ctx = ctx,
        .session_id = session_id,
        .timezone_offset_minutes = timezone_offset_minutes,
        .sink = sink,
    };

    try provider.withJsonDocumentReader(
        allocator,
        ctx,
        runtime,
        file_path,
        .{
            .max_bytes = 64 * 1024 * 1024,
            .open_error_message = "unable to open amp session file",
            .stat_error_message = "unable to stat amp session file",
        },
        &handler,
        Handler.run,
    );
}

test "amp parser emits usage events from ledger + message usage" {
    const allocator = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events = std.ArrayList(model.TokenUsageEvent){};
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    const ctx = provider.ParseContext{
        .provider_name = "amp-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const runtime = provider.ParseRuntime{ .io = io_single.io() };

    try parseSessionFile(
        worker_allocator,
        &ctx,
        &runtime,
        "amp-fixture",
        "fixtures/amp/basic.json",
        null,
        0,
        sink,
    );

    try testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try testing.expectEqualStrings("amp-fixture", event.session_id);
    try testing.expectEqualStrings("claude-haiku-4-5-20251001", event.model);
    try testing.expectEqual(@as(u64, 9), event.usage.input_tokens);
    try testing.expectEqual(@as(u64, 100), event.usage.cache_creation_input_tokens);
    try testing.expectEqual(@as(u64, 200), event.usage.cached_input_tokens);
    try testing.expectEqual(@as(u64, 50), event.usage.output_tokens);
    try testing.expectEqual(@as(u64, 359), event.usage.total_tokens);
    try testing.expectEqualStrings("2025-12-03T02:18:38.333Z", event.timestamp);
}
