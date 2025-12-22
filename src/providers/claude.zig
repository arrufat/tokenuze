const std = @import("std");

const model = @import("../model.zig");
const RawUsage = model.RawTokenUsage;
const provider = @import("provider.zig");
const MessageDeduper = provider.MessageDeduper;
const ModelState = provider.ModelState;

const CLAUDE_USAGE_FIELDS = [_]provider.UsageFieldDescriptor{
    .{ .key = "input_tokens", .field = .input_tokens },
    .{ .key = "cache_creation_input_tokens", .field = .cache_creation_input_tokens },
    .{ .key = "cache_read_input_tokens", .field = .cached_input_tokens },
    .{ .key = "output_tokens", .field = .output_tokens },
    .{ .key = "input_tokens", .field = .total_tokens, .mode = .add },
    .{ .key = "cache_creation_input_tokens", .field = .total_tokens, .mode = .add },
    .{ .key = "cache_read_input_tokens", .field = .total_tokens, .mode = .add },
    .{ .key = "output_tokens", .field = .total_tokens, .mode = .add },
};

const ProviderExports = provider.makeProvider(.{
    .name = "claude",
    .sessions_dir_suffix = "/.claude/projects",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".jsonl",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseClaudeSessionFile,
    .requires_deduper = true,
});

pub const collect = ProviderExports.collect;
pub const loadPricingData = ProviderExports.loadPricingData;
pub const sessionsPath = ProviderExports.sessionsPath;

const EventBuilder = struct {
    handler: *LineHandler,
    allocator: std.mem.Allocator,
    timestamp: ?provider.TimestampInfo = null,
    request_id: ?[]const u8 = null,
    message_id: ?[]const u8 = null,
    usage: ?RawUsage = null,
    type_is_assistant: bool = false,

    fn deinit(self: *EventBuilder) void {
        if (self.timestamp) |info| {
            self.allocator.free(info.text);
            self.timestamp = null;
        }
        if (self.request_id) |id| {
            self.allocator.free(id);
            self.request_id = null;
        }
        if (self.message_id) |id| {
            self.allocator.free(id);
            self.message_id = null;
        }
        self.usage = null;
        self.type_is_assistant = false;
    }

    fn replaceSlice(self: *EventBuilder, dest: *?[]const u8, value: []const u8) !void {
        if (dest.*) |existing| self.allocator.free(existing);
        dest.* = try self.allocator.dupe(u8, value);
    }

    fn emit(self: *EventBuilder) !void {
        if (!self.type_is_assistant) return;
        const usage_raw = self.usage orelse return;
        if (!try shouldEmitClaudeMessage(self.handler.deduper, self.request_id, self.message_id)) {
            return;
        }
        const usage = model.TokenUsage.fromRaw(usage_raw);
        try provider.emitUsageEventWithTimestamp(
            self.handler.ctx,
            self.handler.allocator,
            self.handler.model_state,
            self.handler.sink,
            self.handler.session_label.*,
            &self.timestamp,
            usage,
            null,
        );
    }
};

fn parseClaudeSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    var session_label = session_id;
    var session_label_overridden = false;
    var model_state = ModelState{};

    var handler = LineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .file_path = file_path,
        .deduper = deduper,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .sink = sink,
        .model_state = &model_state,
    };

    try provider.streamJsonLines(
        allocator,
        ctx,
        runtime,
        file_path,
        .{
            .max_bytes = 1024 * 1024 * 1024,
            .open_error_message = "unable to open claude session file",
            .read_error_message = "error while reading claude session stream",
            .advance_error_message = "error while advancing claude session stream",
        },
        &handler,
        LineHandler.handle,
    );
}

const LineHandler = struct {
    ctx: *const provider.ParseContext,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
    model_state: *ModelState,

    fn handle(self: *LineHandler, line: []const u8, line_index: usize) !void {
        self.processLine(line) catch |err| {
            std.log.warn(
                "{s}: failed to parse claude session file '{s}' line {d} ({s})",
                .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
            );
        };
    }

    fn processLine(self: *LineHandler, line: []const u8) !void {
        try provider.parseJsonLine(self.allocator, line, self, processRecord);
    }

    fn processRecord(self: *LineHandler, allocator: std.mem.Allocator, reader: *std.json.Reader) !void {
        var builder = EventBuilder{
            .handler = self,
            .allocator = self.allocator,
        };
        defer builder.deinit();

        try provider.jsonWalkObject(allocator, reader, &builder, parseClaudeRecordField);
        try builder.emit();
    }
};

fn parseClaudeRecordField(
    builder: *EventBuilder,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "sessionId")) {
        try provider.overrideSessionLabelFromReader(
            builder.handler.allocator,
            reader,
            builder.handler.session_label,
            builder.handler.session_label_overridden,
        );
        return;
    }
    if (std.mem.eql(u8, key, "timestamp")) {
        try provider.updateTimestampFromReader(
            builder.handler.allocator,
            reader,
            builder.handler.timezone_offset_minutes,
            &builder.timestamp,
        );
        return;
    }
    if (std.mem.eql(u8, key, "requestId")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        try builder.replaceSlice(&builder.request_id, token.view());
        return;
    }
    if (std.mem.eql(u8, key, "type")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        builder.type_is_assistant = std.mem.eql(u8, token.view(), "assistant");
        return;
    }
    if (std.mem.eql(u8, key, "message")) {
        try parseClaudeMessageObject(builder, allocator, reader);
        return;
    }

    try reader.skipValue();
}

fn parseClaudeMessageObject(
    builder: *EventBuilder,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
) !void {
    try provider.jsonWalkOptionalObject(allocator, reader, builder, parseClaudeMessageField);
}

fn parseClaudeMessageField(
    builder: *EventBuilder,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "id")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        try builder.replaceSlice(&builder.message_id, token.view());
        return;
    }
    if (std.mem.eql(u8, key, "model")) {
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        _ = builder.handler.ctx.captureModel(builder.handler.allocator, builder.handler.model_state, token) catch |err| {
            builder.handler.ctx.logWarning(builder.handler.file_path, "failed to capture model", err);
            return;
        };
        return;
    }
    if (std.mem.eql(u8, key, "usage")) {
        builder.usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, CLAUDE_USAGE_FIELDS[0..]);
        return;
    }

    try reader.skipValue();
}

fn shouldEmitClaudeMessage(
    deduper: ?*MessageDeduper,
    request_id: ?[]const u8,
    message_id: ?[]const u8,
) !bool {
    const dedupe = deduper orelse return true;
    const msg_id = message_id orelse return true;
    const req_id = request_id orelse return true;
    var hash = std.hash.Wyhash.hash(0, msg_id);
    hash = std.hash.Wyhash.hash(hash, req_id);
    return try dedupe.mark(hash);
}

test "claude parser emits assistant usage events and respects overrides" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    var deduper = try MessageDeduper.init(worker_allocator);
    defer deduper.deinit();

    const ctx = provider.ParseContext{
        .provider_name = "claude-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const runtime = provider.ParseRuntime{ .io = io_single.io() };

    try parseClaudeSessionFile(
        worker_allocator,
        &ctx,
        &runtime,
        "claude-fixture",
        "fixtures/claude/basic.jsonl",
        &deduper,
        0,
        sink,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("claude-session", event.session_id);
    try std.testing.expectEqualStrings("claude-3-5-sonnet", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 1500), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 100), event.usage.cache_creation_input_tokens);
    try std.testing.expectEqual(@as(u64, 250), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 600), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 1850), event.display_input_tokens);
}
