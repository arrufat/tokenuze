const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const RawUsage = model.RawTokenUsage;
const ModelState = provider.ModelState;

const GEMINI_USAGE_FIELDS = [_]provider.UsageFieldDescriptor{
    .{ .key = "input", .field = .input_tokens },
    .{ .key = "cached", .field = .cached_input_tokens },
    .{ .key = "output", .field = .output_tokens },
    .{ .key = "tool", .field = .output_tokens, .mode = .add },
    .{ .key = "thoughts", .field = .reasoning_output_tokens },
};

fn recomputeTotal(usage: *RawUsage) void {
    var total = usage.input_tokens +| usage.cache_creation_input_tokens;
    total = total +| usage.cached_input_tokens;
    total = total +| usage.output_tokens;
    total = total +| usage.reasoning_output_tokens;
    usage.total_tokens = total;
}

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
    .parse_session_fn = parseSessionFile,
});

pub const collect = ProviderExports.collect;
pub const loadPricingData = ProviderExports.loadPricingData;

const ParseState = struct {
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    file_path: []const u8,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
    previous_totals: *?RawUsage,
    model_state: *ModelState,
};

const MessageContext = struct {
    state: *ParseState,
    timestamp: ?provider.TimestampInfo = null,
    usage: ?RawUsage = null,

    fn deinit(self: *MessageContext) void {
        self.clearTimestamp();
    }

    fn clearTimestamp(self: *MessageContext) void {
        if (self.timestamp) |info| {
            self.state.allocator.free(info.text);
            self.timestamp = null;
        }
    }
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
    var session_label = session_id;
    var session_label_overridden = false;
    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    var handler = LineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .file_path = file_path,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .sink = sink,
        .previous_totals = &previous_totals,
        .model_state = &model_state,
    };

    try provider.streamJsonLines(
        allocator,
        ctx,
        file_path,
        .{
            .max_bytes = 128 * 1024 * 1024,
            .mode = .document,
            .trim_lines = false,
            .skip_empty = false,
            .open_error_message = "failed to open gemini session file",
            .read_error_message = "error while reading gemini session stream",
            .advance_error_message = "error while advancing gemini session stream",
        },
        &handler,
        LineHandler.handle,
    );
}

const LineHandler = struct {
    ctx: *const provider.ParseContext,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
    previous_totals: *?RawUsage,
    model_state: *ModelState,

    fn handle(self: *LineHandler, line: []const u8, line_index: usize) !void {
        provider.parseJsonLine(self.allocator, line, self, parseDocument) catch |err| {
            std.log.warn(
                "{s}: failed to parse gemini session file '{s}' line {d} ({s})",
                .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
            );
        };
    }

    fn parseDocument(self: *LineHandler, allocator: std.mem.Allocator, reader: *std.json.Reader) !void {
        var state = ParseState{
            .allocator = allocator,
            .ctx = self.ctx,
            .file_path = self.file_path,
            .session_label = self.session_label,
            .session_label_overridden = self.session_label_overridden,
            .timezone_offset_minutes = self.timezone_offset_minutes,
            .sink = self.sink,
            .previous_totals = self.previous_totals,
            .model_state = self.model_state,
        };

        try provider.jsonWalkObject(allocator, reader, &state, parseRootField);
    }
};

fn parseRootField(
    state: *ParseState,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "sessionId")) {
        try provider.overrideSessionLabelFromReader(
            allocator,
            reader,
            state.session_label,
            state.session_label_overridden,
        );
        return;
    }

    if (std.mem.eql(u8, key, "messages")) {
        try parseMessagesArray(state, allocator, reader);
        return;
    }

    try reader.skipValue();
}

fn parseMessagesArray(
    state: *ParseState,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
) !void {
    try provider.jsonWalkArrayObjects(allocator, reader, state, parseMessageObject);
}

fn parseMessageObject(
    state: *ParseState,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    _: usize,
) !void {
    var context = MessageContext{ .state = state };
    defer context.deinit();
    try provider.jsonWalkObject(allocator, reader, &context, parseMessageField);
    try emitMessage(&context);
}

fn parseMessageField(
    context: *MessageContext,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "timestamp")) {
        try provider.updateTimestampFromReader(
            context.state.allocator,
            reader,
            context.state.timezone_offset_minutes,
            &context.timestamp,
        );
        return;
    }
    if (std.mem.eql(u8, key, "model")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        _ = context.state.ctx.captureModel(context.state.allocator, context.state.model_state, token) catch |err| {
            context.state.ctx.logWarning(context.state.file_path, "failed to capture model", err);
            return;
        };
        return;
    }
    if (std.mem.eql(u8, key, "tokens")) {
        context.usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, GEMINI_USAGE_FIELDS[0..]);
        if (context.usage) |*usage| {
            recomputeTotal(usage);
        }
        return;
    }

    try reader.skipValue();
}

fn emitMessage(context: *MessageContext) !void {
    const usage_raw = context.usage orelse return;

    if (context.state.previous_totals.*) |prev| {
        if (std.meta.eql(prev, usage_raw)) {
            return;
        }
    }

    var delta = model.TokenUsage.deltaFrom(usage_raw, context.state.previous_totals.*);
    context.state.ctx.normalizeUsageDelta(&delta);
    context.state.previous_totals.* = usage_raw;

    try provider.emitUsageEventWithTimestamp(
        context.state.ctx,
        context.state.allocator,
        context.state.model_state,
        context.state.sink,
        context.state.session_label.*,
        &context.timestamp,
        delta,
        null,
    );
}

test "gemini parser converts message totals into usage deltas" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    const ctx = provider.ParseContext{
        .provider_name = "gemini-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    try parseSessionFile(
        worker_allocator,
        &ctx,
        "gemini-fixture",
        "fixtures/gemini/basic.json",
        null,
        0,
        sink,
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
    try std.testing.expectEqual(@as(u64, 4645), event.usage.total_tokens);
    try std.testing.expectEqual(@as(u64, 4000), event.display_input_tokens);
}
