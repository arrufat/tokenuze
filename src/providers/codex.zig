const std = @import("std");

const model = @import("../model.zig");
const RawUsage = model.RawTokenUsage;
const provider = @import("provider.zig");
const TokenSlice = provider.JsonTokenSlice;
const ModelState = provider.ModelState;

const token_count_type = "token_count";

const CODEX_USAGE_FIELDS = [_]provider.UsageFieldDescriptor{
    .{ .key = "input_tokens", .field = .input_tokens },
    .{ .key = "cached_input_tokens", .field = .cached_input_tokens },
    .{ .key = "cache_read_input_tokens", .field = .cache_read_input_tokens },
    .{ .key = "output_tokens", .field = .output_tokens },
    .{ .key = "reasoning_output_tokens", .field = .reasoning_output_tokens },
    .{ .key = "total_tokens", .field = .total_tokens },
};

const fallback_pricing = [_]provider.FallbackPricingEntry{
    .{ .name = "gpt-5", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
    .{ .name = "gpt-5-codex", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
};

const ProviderExports = provider.makeProvider(.{
    .name = "codex",
    .sessions_dir_suffix = "/.codex/sessions",
    .legacy_fallback_model = "gpt-5",
    .fallback_pricing = fallback_pricing[0..],
    .cached_counts_overlap_input = true,
    .parse_session_fn = parseSessionFile,
});

pub const collect = ProviderExports.collect;
pub const streamEvents = ProviderExports.streamEvents;
pub const loadPricingData = ProviderExports.loadPricingData;
pub const EventConsumer = ProviderExports.EventConsumer;
pub const sessionsPath = ProviderExports.sessionsPath;

const PayloadResult = struct {
    model: ?TokenSlice = null,
    last_usage: ?RawUsage = null,
    total_usage: ?RawUsage = null,
    is_token_count: bool = false,

    fn deinit(self: *PayloadResult, allocator: std.mem.Allocator) void {
        if (self.model) |*tok| tok.deinit(allocator);
        self.* = .{};
    }
};

const ObjectFieldContext = struct {
    payload: *PayloadResult,
    timestamp_token: *?TokenSlice,
    is_turn_context: *bool,
    is_event_msg: *bool,
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
    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    var handler = LineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .file_path = file_path,
        .session_id = session_id,
        .sink = sink,
        .previous_totals = &previous_totals,
        .model_state = &model_state,
        .timezone_offset_minutes = timezone_offset_minutes,
    };

    try provider.streamJsonLines(
        allocator,
        ctx,
        runtime,
        file_path,
        .{
            .max_bytes = 1024 * 1024 * 1024,
            .open_error_message = "unable to open session file",
            .read_error_message = "error while reading session stream",
            .advance_error_message = "error while advancing session stream",
        },
        &handler,
        LineHandler.handle,
    );
}

const LineHandler = struct {
    ctx: *const provider.ParseContext,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    session_id: []const u8,
    sink: provider.EventSink,
    previous_totals: *?RawUsage,
    model_state: *ModelState,
    timezone_offset_minutes: i32,

    fn handle(self: *LineHandler, line: []const u8, line_index: usize) !void {
        provider.parseJsonLine(self.allocator, line, self, processSessionLine) catch |err| {
            std.log.warn(
                "{s}: failed to parse codex session file '{s}' line {d} ({s})",
                .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
            );
        };
    }

    fn processSessionLine(self: *LineHandler, allocator: std.mem.Allocator, reader: *std.json.Reader) !void {
        var payload_result = PayloadResult{};
        defer payload_result.deinit(allocator);
        var timestamp_token: ?TokenSlice = null;
        defer if (timestamp_token) |*tok| tok.deinit(allocator);
        var is_turn_context = false;
        var is_event_msg = false;

        const context = ObjectFieldContext{
            .payload = &payload_result,
            .timestamp_token = &timestamp_token,
            .is_turn_context = &is_turn_context,
            .is_event_msg = &is_event_msg,
        };
        try provider.jsonWalkObject(allocator, reader, context, parseObjectField);

        if (timestamp_token == null) return;

        if (is_turn_context) {
            if (payload_result.model) |token| {
                var model_token = token;
                payload_result.model = null;
                _ = self.ctx.captureModel(allocator, self.model_state, model_token) catch |err| {
                    self.ctx.logWarning(self.file_path, "failed to capture model", err);
                    model_token.deinit(self.allocator);
                    return;
                };
                model_token.deinit(self.allocator);
            }
            return;
        }

        if (!is_event_msg) return;

        if (!payload_result.is_token_count) return;

        var raw_timestamp = timestamp_token.?;
        timestamp_token = null;
        const timestamp_info = try provider.timestampFromSlice(self.allocator, raw_timestamp.view(), self.timezone_offset_minutes) orelse {
            raw_timestamp.deinit(self.allocator);
            return;
        };
        raw_timestamp.deinit(self.allocator);

        var delta_usage: ?model.TokenUsage = null;
        if (payload_result.last_usage) |last_usage| {
            delta_usage = model.TokenUsage.fromRaw(last_usage);
        } else if (payload_result.total_usage) |total_usage| {
            delta_usage = model.TokenUsage.deltaFrom(total_usage, self.previous_totals.*);
        }
        if (payload_result.total_usage) |total_usage| {
            self.previous_totals.* = total_usage;
        }

        if (delta_usage == null) return;

        var delta = delta_usage.?;
        self.ctx.normalizeUsageDelta(&delta);
        if (!provider.shouldEmitUsage(delta)) {
            return;
        }

        const payload_model = payload_result.model;
        const resolved = (try self.ctx.requireModel(self.allocator, self.model_state, payload_model)) orelse return;
        if (payload_result.model) |token| {
            var model_token = token;
            payload_result.model = null;
            model_token.deinit(self.allocator);
        }

        const event = model.TokenUsageEvent{
            .session_id = self.session_id,
            .timestamp = timestamp_info.text,
            .local_iso_date = timestamp_info.local_iso_date,
            .model = resolved.name,
            .usage = delta,
            .is_fallback = resolved.is_fallback,
            .display_input_tokens = provider.ParseContext.computeDisplayInput(delta),
        };
        try self.sink.emit(event);
    }
};

fn parseObjectField(
    context: ObjectFieldContext,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "type")) {
        var value = try provider.jsonReadStringToken(allocator, reader);
        defer value.deinit(allocator);
        const type_slice = value.view();
        if (std.mem.eql(u8, type_slice, "turn_context")) {
            context.is_turn_context.* = true;
            context.is_event_msg.* = false;
        } else if (std.mem.eql(u8, type_slice, "event_msg")) {
            context.is_event_msg.* = true;
            context.is_turn_context.* = false;
        }
        return;
    }

    if (std.mem.eql(u8, key, "timestamp")) {
        try provider.replaceJsonToken(context.timestamp_token, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }

    if (std.mem.eql(u8, key, "payload")) {
        try parsePayload(allocator, reader, context.payload);
        return;
    }

    try reader.skipValue();
}

fn parsePayload(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    payload_result: *PayloadResult,
) !void {
    try provider.jsonWalkOptionalObject(allocator, reader, payload_result, parsePayloadField);
}

fn parsePayloadField(
    payload_result: *PayloadResult,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (try parseSharedPayloadField(allocator, reader, key, payload_result)) return;

    if (std.mem.eql(u8, key, "type")) {
        var value = try provider.jsonReadStringToken(allocator, reader);
        defer value.deinit(allocator);
        payload_result.is_token_count = std.mem.eql(u8, value.view(), token_count_type);
        return;
    }

    if (std.mem.eql(u8, key, "info")) {
        try parseInfoObject(allocator, reader, payload_result);
        return;
    }

    try reader.skipValue();
}

fn parseInfoObject(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    payload_result: *PayloadResult,
) !void {
    try provider.jsonWalkOptionalObject(allocator, reader, payload_result, parseInfoField);
}

fn parseInfoField(
    payload_result: *PayloadResult,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (try parseSharedPayloadField(allocator, reader, key, payload_result)) return;

    if (std.mem.eql(u8, key, "last_token_usage")) {
        payload_result.last_usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, CODEX_USAGE_FIELDS[0..]);
        return;
    }
    if (std.mem.eql(u8, key, "total_token_usage")) {
        payload_result.total_usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, CODEX_USAGE_FIELDS[0..]);
        return;
    }

    try reader.skipValue();
}

fn parseSharedPayloadField(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
    payload_result: *PayloadResult,
) !bool {
    if (provider.isModelKey(key)) {
        const maybe_token = try provider.jsonReadOptionalStringToken(allocator, reader);
        if (maybe_token) |token| provider.captureModelToken(&payload_result.model, allocator, token);
        return true;
    }
    if (std.mem.eql(u8, key, "metadata")) {
        try provider.captureModelValue(allocator, reader, &payload_result.model);
        return true;
    }
    return false;
}

test "codex parser emits usage events from token_count entries" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events = std.ArrayList(model.TokenUsageEvent){};
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    const ctx = provider.ParseContext{
        .provider_name = "codex-test",
        .legacy_fallback_model = "gpt-5",
        .cached_counts_overlap_input = true,
    };
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const runtime = provider.ParseRuntime{ .io = io_single.io() };

    try parseSessionFile(
        worker_allocator,
        &ctx,
        &runtime,
        "codex-fixture",
        "fixtures/codex/basic.jsonl",
        null,
        0,
        sink,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("codex-fixture", event.session_id);
    try std.testing.expectEqualStrings("gpt-5-codex", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 1000), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 200), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 50), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 1450), event.usage.total_tokens);
    try std.testing.expectEqual(@as(u64, 1200), event.display_input_tokens);
}
