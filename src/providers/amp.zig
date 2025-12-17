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
    var state = ParseState{
        .allocator = allocator,
        .ctx = ctx,
        .session_id = session_id,
        .timezone_offset_minutes = timezone_offset_minutes,
        .sink = sink,
        .usage_by_message = std.AutoHashMap(u64, UsageEntry).init(allocator),
    };
    defer state.usage_by_message.deinit();

    try provider.withJsonObjectReader(
        allocator,
        ctx,
        runtime,
        file_path,
        .{
            .max_bytes = 64 * 1024 * 1024,
            .open_error_message = "unable to open amp session file",
            .stat_error_message = "unable to stat amp session file",
        },
        &state,
        ParseState.parseRootObject,
    );
}

const ParseState = struct {
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    session_id: []const u8,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
    usage_by_message: std.AutoHashMap(u64, UsageEntry),

    fn parseRootObject(self: *ParseState, scratch: std.mem.Allocator, reader: *std.json.Reader) !void {
        try provider.jsonWalkObject(scratch, reader, self, handleRootField);
    }

    fn handleRootField(self: *ParseState, scratch: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "messages")) {
            try provider.jsonWalkArrayObjects(scratch, reader, self, handleMessageObject);
            return;
        }
        if (std.mem.eql(u8, key, "usageLedger")) {
            try provider.jsonWalkOptionalObject(scratch, reader, self, handleLedgerField);
            return;
        }
        try reader.skipValue();
    }

    fn handleMessageObject(self: *ParseState, scratch: std.mem.Allocator, reader: *std.json.Reader, _: usize) !void {
        var rec = MessageRecord{};
        try provider.jsonWalkObject(scratch, reader, &rec, MessageRecord.handleField);
        const message_id = rec.message_id orelse return;
        if (!provider.shouldEmitUsage(rec.entry.usage)) return;
        try self.usage_by_message.put(message_id, rec.entry);
    }

    fn handleLedgerField(self: *ParseState, scratch: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (!std.mem.eql(u8, key, "events")) {
            try reader.skipValue();
            return;
        }
        try provider.jsonWalkArrayObjects(scratch, reader, self, handleLedgerEventObject);
    }

    fn handleLedgerEventObject(self: *ParseState, scratch: std.mem.Allocator, reader: *std.json.Reader, _: usize) !void {
        var rec = LedgerEventRecord{ .timezone_offset_minutes = self.timezone_offset_minutes };
        try provider.jsonWalkObject(scratch, reader, &rec, LedgerEventRecord.handleField);

        const target_id = rec.to_message_id orelse rec.from_message_id orelse return;
        var timestamp_info = rec.timestamp_info orelse return;
        var timestamp_moved = false;
        defer if (!timestamp_moved and timestamp_info.text.len > 0) scratch.free(timestamp_info.text);

        var usage = model.TokenUsage{
            .input_tokens = rec.tokens.input,
            .output_tokens = rec.tokens.output,
            .total_tokens = rec.tokens.input + rec.tokens.output,
        };

        var raw_model: ?[]const u8 = rec.model;
        if (self.usage_by_message.get(target_id)) |msg_usage| {
            usage = msg_usage.usage;
            if (msg_usage.model) |m| raw_model = m;
        }
        const model_slice = raw_model orelse return;

        var timestamp_slot: ?provider.TimestampInfo = timestamp_info;
        var model_state = provider.ModelState{};
        provider.emitUsageEventWithTimestamp(
            self.ctx,
            scratch,
            &model_state,
            self.sink,
            self.session_id,
            &timestamp_slot,
            usage,
            model_slice,
        ) catch return;

        timestamp_slot = null;
        timestamp_moved = true;
    }
};

const MessageRecord = struct {
    message_id: ?u64 = null,
    entry: UsageEntry = .{},
    total_input_override: ?u64 = null,
    total_tokens_override: ?u64 = null,

    fn handleField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "messageId")) {
            self.message_id = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "usage")) {
            try provider.jsonWalkOptionalObject(allocator, reader, self, handleUsageField);
            self.finalize();
            return;
        }
        try reader.skipValue();
    }

    fn handleUsageField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "model")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            const trimmed = std.mem.trim(u8, token.view(), " \r\n\t");
            if (trimmed.len == 0) return;
            self.entry.model = try allocator.dupe(u8, trimmed);
            return;
        }
        if (std.mem.eql(u8, key, "inputTokens")) {
            self.entry.usage.input_tokens = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "cacheCreationInputTokens")) {
            self.entry.usage.cache_creation_input_tokens = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "cacheReadInputTokens")) {
            self.entry.usage.cached_input_tokens = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "outputTokens")) {
            self.entry.usage.output_tokens = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "reasoningOutputTokens")) {
            self.entry.usage.reasoning_output_tokens = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "totalInputTokens")) {
            self.total_input_override = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "totalTokens")) {
            self.total_tokens_override = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        try reader.skipValue();
    }

    fn finalize(self: *MessageRecord) void {
        const total_input = self.total_input_override orelse
            (self.entry.usage.input_tokens + self.entry.usage.cache_creation_input_tokens + self.entry.usage.cached_input_tokens);
        self.entry.usage.total_tokens = self.total_tokens_override orelse
            (total_input + self.entry.usage.output_tokens + self.entry.usage.reasoning_output_tokens);
    }
};

const LedgerEventRecord = struct {
    timezone_offset_minutes: i32,
    to_message_id: ?u64 = null,
    from_message_id: ?u64 = null,
    timestamp_info: ?provider.TimestampInfo = null,
    tokens: LedgerTokens = .{},
    model: ?[]const u8 = null,

    fn handleField(self: *LedgerEventRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "toMessageId")) {
            self.to_message_id = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "fromMessageId")) {
            self.from_message_id = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "timestamp")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            self.timestamp_info = (try provider.timestampFromSlice(allocator, token.view(), self.timezone_offset_minutes)) orelse return;
            return;
        }
        if (std.mem.eql(u8, key, "tokens")) {
            try provider.jsonWalkOptionalObject(allocator, reader, self, handleTokensField);
            return;
        }
        if (std.mem.eql(u8, key, "model")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            const trimmed = std.mem.trim(u8, token.view(), " \r\n\t");
            if (trimmed.len == 0) return;
            self.model = try allocator.dupe(u8, trimmed);
            return;
        }
        try reader.skipValue();
    }

    fn handleTokensField(self: *LedgerEventRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "input")) {
            self.tokens.input = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "output")) {
            self.tokens.output = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        try reader.skipValue();
    }
};

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
