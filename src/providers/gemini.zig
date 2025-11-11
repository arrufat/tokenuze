const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const RawUsage = model.RawTokenUsage;
const ModelState = provider.ModelState;
const TokenSlice = provider.JsonTokenSlice;

const GEMINI_USAGE_FIELDS = [_]provider.UsageFieldDescriptor{
    .{ .key = "input", .field = .input_tokens },
    .{ .key = "cached", .field = .cached_input_tokens },
    .{ .key = "output", .field = .output_tokens },
    .{ .key = "tool", .field = .output_tokens, .mode = .add },
    .{ .key = "thoughts", .field = .reasoning_output_tokens },
    .{ .key = "total", .field = .total_tokens },
};

const fallback_pricing = [_]provider.FallbackPricingEntry{
    .{ .name = "gemini-2.5-pro", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
    .{ .name = "gemini-flash-latest", .pricing = .{
        .input_cost_per_m = 0.30,
        .cache_creation_cost_per_m = 0.30,
        .cached_input_cost_per_m = 0.075,
        .output_cost_per_m = 2.50,
    } },
    .{ .name = "gemini-1.5-pro", .pricing = .{
        .input_cost_per_m = 3.50,
        .cache_creation_cost_per_m = 3.50,
        .cached_input_cost_per_m = 3.50,
        .output_cost_per_m = 10.50,
    } },
    .{ .name = "gemini-1.5-flash", .pricing = .{
        .input_cost_per_m = 0.35,
        .cache_creation_cost_per_m = 0.35,
        .cached_input_cost_per_m = 0.35,
        .output_cost_per_m = 1.05,
    } },
};

const ProviderExports = provider.makeProvider(.{
    .name = "gemini",
    .sessions_dir_suffix = "/.gemini/tmp",
    .legacy_fallback_model = null,
    .fallback_pricing = fallback_pricing[0..],
    .session_file_ext = ".json",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseGeminiSessionFile,
});

pub const collect = ProviderExports.collect;
pub const loadPricingData = ProviderExports.loadPricingData;

const GeminiParseState = struct {
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    file_path: []const u8,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
    previous_totals: *?RawUsage,
    model_state: *ModelState,
};

const GeminiMessage = struct {
    timestamp: ?TokenSlice = null,
    model: ?TokenSlice = null,
    usage: ?RawUsage = null,

    fn deinit(self: *GeminiMessage, allocator: std.mem.Allocator) void {
        if (self.timestamp) |*tok| tok.deinit(allocator);
        if (self.model) |*tok| tok.deinit(allocator);
        self.* = .{};
    }
};

fn parseGeminiSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*provider.MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    _ = deduper;
    var session_label = session_id;
    var session_label_overridden = false;
    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    const file_data = std.fs.cwd().readFileAlloc(file_path, allocator, std.Io.Limit.limited(32 * 1024 * 1024)) catch |err| {
        ctx.logWarning(file_path, "failed to read gemini session file", err);
        return;
    };
    defer allocator.free(file_data);

    var slice_reader = std.Io.Reader.fixed(file_data);
    var json_reader = std.json.Reader.init(allocator, &slice_reader);
    defer json_reader.deinit();

    if ((try json_reader.next()) != .object_begin) return;

    var state = GeminiParseState{
        .allocator = allocator,
        .ctx = ctx,
        .file_path = file_path,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .events = events,
        .previous_totals = &previous_totals,
        .model_state = &model_state,
    };

    provider.jsonWalkObject(allocator, &json_reader, &state, parseGeminiRootField) catch |err| {
        ctx.logWarning(file_path, "failed to parse gemini session file", err);
        return;
    };
}

fn parseGeminiRootField(
    state: *GeminiParseState,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "sessionId")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        provider.overrideSessionLabelFromSlice(
            allocator,
            state.session_label,
            state.session_label_overridden,
            token.view(),
        );
        return;
    }

    if (std.mem.eql(u8, key, "messages")) {
        try parseGeminiMessagesArray(state, allocator, reader);
        return;
    }

    try reader.skipValue();
}

fn parseGeminiMessagesArray(
    state: *GeminiParseState,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
) !void {
    const peek = try reader.peekNextTokenType();
    if (peek == .null) {
        _ = try reader.next();
        return;
    }
    if (peek != .array_begin) {
        try reader.skipValue();
        return;
    }

    _ = try reader.next();

    while (true) {
        switch (try reader.peekNextTokenType()) {
            .array_end => {
                _ = try reader.next();
                return;
            },
            .object_begin => {
                _ = try reader.next();
                var message = GeminiMessage{};
                defer message.deinit(allocator);
                try provider.jsonWalkObject(allocator, reader, &message, parseGeminiMessageField);
                try emitGeminiMessage(state, &message);
            },
            else => {
                try reader.skipValue();
            },
        }
    }
}

fn parseGeminiMessageField(
    message: *GeminiMessage,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "timestamp")) {
        provider.replaceJsonToken(&message.timestamp, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "model")) {
        provider.replaceJsonToken(&message.model, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "tokens")) {
        message.usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, GEMINI_USAGE_FIELDS[0..]);
        return;
    }

    try reader.skipValue();
}

fn emitGeminiMessage(state: *GeminiParseState, message: *GeminiMessage) !void {
    const usage_raw = message.usage orelse return;

    _ = state.ctx.captureModel(state.allocator, state.model_state, message.model) catch |err| {
        state.ctx.logWarning(state.file_path, "failed to capture model", err);
        return;
    };

    var delta = model.TokenUsage.deltaFrom(usage_raw, state.previous_totals.*);
    state.ctx.normalizeUsageDelta(&delta);
    state.previous_totals.* = usage_raw;

    if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
        return;
    }

    const resolved_model = (try state.ctx.requireModel(state.allocator, state.model_state, null)) orelse return;

    const timestamp_token = message.timestamp orelse return;
    const timestamp_info = try provider.timestampFromSlice(
        state.allocator,
        timestamp_token.view(),
        state.timezone_offset_minutes,
    ) orelse return;

    const event = model.TokenUsageEvent{
        .session_id = state.session_label.*,
        .timestamp = timestamp_info.text,
        .local_iso_date = timestamp_info.local_iso_date,
        .model = resolved_model.name,
        .usage = delta,
        .is_fallback = resolved_model.is_fallback,
        .display_input_tokens = state.ctx.computeDisplayInput(delta),
    };
    try state.events.append(state.allocator, event);
}

test "gemini parser converts message totals into usage deltas" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);

    const ctx = provider.ParseContext{
        .provider_name = "gemini-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    try parseGeminiSessionFile(
        worker_allocator,
        &ctx,
        "gemini-fixture",
        "fixtures/gemini/basic.json",
        null,
        0,
        &events,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("gem-session", event.session_id);
    try std.testing.expectEqualStrings("gemini-1.5-pro", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 4000), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 500), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 125), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 20), event.usage.reasoning_output_tokens);
    try std.testing.expectEqual(@as(u64, 4000), event.display_input_tokens);
}
