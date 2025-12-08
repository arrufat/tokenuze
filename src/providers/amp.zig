const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");
const test_helpers = @import("test_helpers.zig");
const testing = std.testing;

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
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*provider.MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    _ = deduper;
    const payload = try std.fs.cwd().readFileAlloc(file_path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(payload);

    try parseThreadPayload(allocator, ctx, session_id, payload, timezone_offset_minutes, sink);
}

fn parseThreadPayload(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    session_id: []const u8,
    payload: []const u8,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |o| o,
        else => return,
    };

    const messages_val = root.get("messages") orelse return;
    const ledger_val = root.get("usageLedger") orelse return;

    const messages = switch (messages_val) {
        .array => |arr| arr,
        else => return,
    };

    const ledger_obj = switch (ledger_val) {
        .object => |o| o,
        else => return,
    };

    const events_val = ledger_obj.get("events") orelse return;
    const events = switch (events_val) {
        .array => |arr| arr,
        else => return,
    };

    var usage_by_message = std.AutoHashMap(u64, UsageEntry).init(allocator);
    defer usage_by_message.deinit();

    for (messages.items) |msg_val| {
        const msg_obj = switch (msg_val) {
            .object => |o| o,
            else => continue,
        };

        const message_id_val = msg_obj.get("messageId") orelse continue;
        const message_id = switch (message_id_val) {
            .integer => |i| @as(u64, @intCast(i)),
            .float => |f| @as(u64, @intFromFloat(f)),
            else => continue,
        };

        const usage_val = msg_obj.get("usage") orelse continue;
        const usage_obj = switch (usage_val) {
            .object => |o| o,
            else => continue,
        };

        var entry = UsageEntry{};
        if (usage_obj.get("model")) |model_val| {
            if (model_val == .string) entry.model = model_val.string;
        }

        var total_input_override: ?u64 = null;
        var total_tokens_override: ?u64 = null;

        entry.usage.input_tokens = if (usage_obj.get("inputTokens")) |val| parseU64(val) else 0;
        entry.usage.cache_creation_input_tokens = if (usage_obj.get("cacheCreationInputTokens")) |val| parseU64(val) else 0;
        entry.usage.cached_input_tokens = if (usage_obj.get("cacheReadInputTokens")) |val| parseU64(val) else 0;
        entry.usage.output_tokens = if (usage_obj.get("outputTokens")) |val| parseU64(val) else 0;
        entry.usage.reasoning_output_tokens = if (usage_obj.get("reasoningOutputTokens")) |val| parseU64(val) else 0;
        total_input_override = if (usage_obj.get("totalInputTokens")) |val| parseU64(val) else null;
        total_tokens_override = if (usage_obj.get("totalTokens")) |val| parseU64(val) else null;

        const total_input = total_input_override orelse
            (entry.usage.input_tokens + entry.usage.cache_creation_input_tokens + entry.usage.cached_input_tokens);
        entry.usage.total_tokens = total_tokens_override orelse
            (total_input + entry.usage.output_tokens + entry.usage.reasoning_output_tokens);

        try usage_by_message.put(message_id, entry);
    }

    for (events.items) |event_val| {
        const event_obj = switch (event_val) {
            .object => |o| o,
            else => continue,
        };

        const target_id = blk: {
            if (event_obj.get("toMessageId")) |v| {
                break :blk parseU64(v);
            }
            if (event_obj.get("fromMessageId")) |v| {
                break :blk parseU64(v);
            }
            continue;
        };

        const ts_val = event_obj.get("timestamp") orelse continue;
        const ts_slice = switch (ts_val) {
            .string => ts_val.string,
            else => continue,
        };
        var timestamp_info = (try provider.timestampFromSlice(allocator, ts_slice, timezone_offset_minutes)) orelse continue;
        var timestamp_moved = false;
        defer if (!timestamp_moved and timestamp_info.text.len > 0) allocator.free(timestamp_info.text);

        var ledger_tokens = LedgerTokens{};
        if (event_obj.get("tokens")) |tokens_val| {
            switch (tokens_val) {
                .object => |tok_obj| {
                    ledger_tokens.input = if (tok_obj.get("input")) |v| parseU64(v) else 0;
                    ledger_tokens.output = if (tok_obj.get("output")) |v| parseU64(v) else 0;
                },
                else => {},
            }
        }

        const ledger_model: ?[]const u8 = blk: {
            if (event_obj.get("model")) |v| switch (v) {
                .string => break :blk v.string,
                else => {},
            };
            break :blk null;
        };

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

        if (raw_model == null) continue;

        var timestamp_slot: ?provider.TimestampInfo = timestamp_info;
        var model_state = provider.ModelState{};
        provider.emitUsageEventWithTimestamp(
            ctx,
            allocator,
            &model_state,
            sink,
            session_id,
            &timestamp_slot,
            usage,
            raw_model.?,
        ) catch continue;

        timestamp_slot = null;
        timestamp_moved = true;
    }
}

fn parseU64(val: std.json.Value) u64 {
    return switch (val) {
        .integer => |i| @as(u64, @intCast(i)),
        .float => |f| @as(u64, @intFromFloat(f)),
        else => 0,
    };
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

    try parseSessionFile(
        worker_allocator,
        &ctx,
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
