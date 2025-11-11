const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const RawUsage = model.RawTokenUsage;
const MessageDeduper = provider.MessageDeduper;
const ModelState = provider.ModelState;
const TokenSlice = provider.JsonTokenSlice;

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

const ClaudeMessage = struct {
    id: ?TokenSlice = null,
    model: ?TokenSlice = null,
    usage: ?RawUsage = null,

    fn deinit(self: *ClaudeMessage, allocator: std.mem.Allocator) void {
        if (self.id) |*tok| tok.deinit(allocator);
        if (self.model) |*tok| tok.deinit(allocator);
        self.* = .{};
    }
};

const ClaudeRecord = struct {
    session_id: ?TokenSlice = null,
    timestamp: ?TokenSlice = null,
    request_id: ?TokenSlice = null,
    type_token: ?TokenSlice = null,
    message: ClaudeMessage = .{},

    fn deinit(self: *ClaudeRecord, allocator: std.mem.Allocator) void {
        if (self.session_id) |*tok| tok.deinit(allocator);
        if (self.timestamp) |*tok| tok.deinit(allocator);
        if (self.request_id) |*tok| tok.deinit(allocator);
        if (self.type_token) |*tok| tok.deinit(allocator);
        self.message.deinit(allocator);
        self.* = .{};
    }
};

fn parseClaudeSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    var session_label = session_id;
    var session_label_overridden = false;
    var model_state = ModelState{};

    var handler = ClaudeLineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .file_path = file_path,
        .deduper = deduper,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .events = events,
        .model_state = &model_state,
    };

    try provider.streamJsonLines(
        allocator,
        ctx,
        file_path,
        .{
            .max_bytes = 128 * 1024 * 1024,
            .open_error_message = "unable to open claude session file",
            .read_error_message = "error while reading claude session stream",
            .advance_error_message = "error while advancing claude session stream",
        },
        &handler,
        ClaudeLineHandler.handle,
    );
}

const ClaudeLineHandler = struct {
    ctx: *const provider.ParseContext,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
    model_state: *ModelState,

    fn handle(self: *ClaudeLineHandler, line: []const u8, line_index: usize) !void {
        self.processLine(line) catch |err| {
            std.log.warn(
                "{s}: failed to parse claude session file '{s}' line {d} ({s})",
                .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
            );
        };
    }

    fn processLine(self: *ClaudeLineHandler, line: []const u8) !void {
        try provider.parseJsonLine(self.allocator, line, self, processRecord);
    }

    fn processRecord(self: *ClaudeLineHandler, allocator: std.mem.Allocator, reader: *std.json.Reader) !void {
        var record = ClaudeRecord{};
        defer record.deinit(allocator);

        try provider.jsonWalkObject(allocator, reader, &record, parseClaudeRecordField);

        try self.emitEvent(&record);
    }

    fn emitEvent(self: *ClaudeLineHandler, record: *ClaudeRecord) !void {
        if (record.session_id) |token| {
            provider.overrideSessionLabelFromSlice(
                self.allocator,
                self.session_label,
                self.session_label_overridden,
                token.view(),
            );
        }

        const type_token = record.type_token orelse return;
        if (!std.mem.eql(u8, type_token.view(), "assistant")) return;

        if (!try shouldEmitClaudeMessage(self.deduper, record.request_id, record.message.id)) {
            return;
        }

        const usage_raw = record.message.usage orelse return;
        const timestamp_token = record.timestamp orelse return;
        const timestamp_info = try provider.timestampFromSlice(
            self.allocator,
            timestamp_token.view(),
            self.timezone_offset_minutes,
        ) orelse return;

        const resolved_model = (try self.ctx.requireModel(self.allocator, self.model_state, record.message.model)) orelse return;

        const usage = model.TokenUsage.fromRaw(usage_raw);
        if (!provider.shouldEmitUsage(usage)) {
            return;
        }

        const event = model.TokenUsageEvent{
            .session_id = self.session_label.*,
            .timestamp = timestamp_info.text,
            .local_iso_date = timestamp_info.local_iso_date,
            .model = resolved_model.name,
            .usage = usage,
            .is_fallback = resolved_model.is_fallback,
            .display_input_tokens = self.ctx.computeDisplayInput(usage),
        };
        try self.events.append(self.allocator, event);
    }
};

fn parseClaudeRecordField(
    record: *ClaudeRecord,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "sessionId")) {
        provider.replaceJsonToken(&record.session_id, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "timestamp")) {
        provider.replaceJsonToken(&record.timestamp, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "requestId")) {
        provider.replaceJsonToken(&record.request_id, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "type")) {
        provider.replaceJsonToken(&record.type_token, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "message")) {
        try parseClaudeMessageObject(record, allocator, reader);
        return;
    }

    try reader.skipValue();
}

fn parseClaudeMessageObject(
    record: *ClaudeRecord,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
) !void {
    const peek = try reader.peekNextTokenType();
    if (peek == .null) {
        _ = try reader.next();
        return;
    }
    if (peek != .object_begin) {
        try reader.skipValue();
        return;
    }

    _ = try reader.next();
    try provider.jsonWalkObject(allocator, reader, &record.message, parseClaudeMessageField);
}

fn parseClaudeMessageField(
    message: *ClaudeMessage,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (std.mem.eql(u8, key, "id")) {
        provider.replaceJsonToken(&message.id, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "model")) {
        provider.replaceJsonToken(&message.model, allocator, try provider.jsonReadStringToken(allocator, reader));
        return;
    }
    if (std.mem.eql(u8, key, "usage")) {
        message.usage = try provider.jsonParseUsageObjectWithDescriptors(allocator, reader, CLAUDE_USAGE_FIELDS[0..]);
        return;
    }

    try reader.skipValue();
}

fn shouldEmitClaudeMessage(
    deduper: ?*MessageDeduper,
    request_token: ?TokenSlice,
    message_token: ?TokenSlice,
) !bool {
    const dedupe = deduper orelse return true;
    const message_id = message_token orelse return true;
    const request_id = request_token orelse return true;
    var hash = std.hash.Wyhash.hash(0, message_id.view());
    hash = std.hash.Wyhash.hash(hash, request_id.view());
    return try dedupe.mark(hash);
}

test "claude parser emits assistant usage events and respects overrides" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);

    var deduper = try MessageDeduper.init(worker_allocator);
    defer deduper.deinit();

    const ctx = provider.ParseContext{
        .provider_name = "claude-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    try parseClaudeSessionFile(
        worker_allocator,
        &ctx,
        "claude-fixture",
        "fixtures/claude/basic.jsonl",
        &deduper,
        0,
        &events,
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
    try std.testing.expectEqual(@as(u64, 1500), event.display_input_tokens);
}
