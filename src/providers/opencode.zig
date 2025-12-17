const std = @import("std");
const ascii = std.ascii;

const model = @import("../model.zig");
const RawUsage = model.RawTokenUsage;
const provider = @import("provider.zig");
const MessageDeduper = provider.MessageDeduper;

const ProviderExports = provider.makeProvider(.{
    .name = "opencode",
    .sessions_dir_suffix = "/.local/share/opencode/storage/session",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".json",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseSessionFile,
});

pub const collect = ProviderExports.collect;
pub const streamEvents = ProviderExports.streamEvents;
pub const loadPricingData = ProviderExports.loadPricingData;
pub const EventConsumer = ProviderExports.EventConsumer;
pub const sessionsPath = ProviderExports.sessionsPath;

const TokenCounts = struct {
    input: u64 = 0,
    output: u64 = 0,
    reasoning: u64 = 0,
    cache_read: u64 = 0,
    cache_write: u64 = 0,

    fn toUsage(self: TokenCounts) model.TokenUsage {
        var total: u64 = 0;
        total = total +| self.input;
        total = total +| self.output;
        total = total +| self.reasoning;
        total = total +| self.cache_read;
        total = total +| self.cache_write;
        const raw = RawUsage{
            .input_tokens = self.input,
            .cache_creation_input_tokens = self.cache_write,
            .cached_input_tokens = self.cache_read,
            .output_tokens = self.output,
            .reasoning_output_tokens = self.reasoning,
            .total_tokens = total,
        };
        return model.TokenUsage.fromRaw(raw);
    }
};

const MessageRecord = struct {
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    sink: provider.EventSink,
    session_label: []const u8,
    timezone_offset_minutes: i32,

    role_assistant: bool = false,
    timestamp_ms: ?u64 = null,
    tokens_present: bool = false,
    counts: TokenCounts = .{},
    model_name: ?[]const u8 = null,

    fn handleField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "role")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            self.role_assistant = ascii.eqlIgnoreCase(token.view(), "assistant");
            return;
        }
        if (std.mem.eql(u8, key, "time")) {
            try provider.jsonWalkOptionalObject(allocator, reader, self, handleTimeField);
            return;
        }
        if (std.mem.eql(u8, key, "tokens")) {
            try walkTokens(self, allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "modelID")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            try self.captureModel(token.view());
            return;
        }
        if (std.mem.eql(u8, key, "model")) {
            try provider.jsonWalkOptionalObject(allocator, reader, self, handleModelField);
            return;
        }
        try reader.skipValue();
    }

    fn captureModel(self: *MessageRecord, raw: []const u8) !void {
        if (self.model_name != null) return;
        const trimmed = std.mem.trim(u8, raw, " \r\n\t");
        if (trimmed.len == 0) return;
        self.model_name = try self.allocator.dupe(u8, trimmed);
    }

    fn handleTimeField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "completed")) {
            self.timestamp_ms = try provider.jsonParseU64Value(allocator, reader);
            return;
        }
        if (std.mem.eql(u8, key, "created")) {
            const value = try provider.jsonParseU64Value(allocator, reader);
            if (self.timestamp_ms == null) {
                self.timestamp_ms = value;
            }
            return;
        }
        try reader.skipValue();
    }

    fn walkTokens(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader) !void {
        const peek = try reader.peekNextTokenType();
        switch (peek) {
            .null => {
                _ = try reader.next();
                return;
            },
            .object_begin => _ = try reader.next(),
            else => {
                try reader.skipValue();
                return;
            },
        }
        self.tokens_present = true;
        try provider.jsonWalkObject(allocator, reader, self, handleTokensField);
    }

    fn handleTokensField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "cache")) {
            try provider.jsonWalkOptionalObject(allocator, reader, self, handleCacheField);
            return;
        }
        if (std.mem.eql(u8, key, "input")) {
            self.counts.input = try provider.jsonParseU64Value(allocator, reader);
        } else if (std.mem.eql(u8, key, "output")) {
            self.counts.output = try provider.jsonParseU64Value(allocator, reader);
        } else if (std.mem.eql(u8, key, "reasoning")) {
            self.counts.reasoning = try provider.jsonParseU64Value(allocator, reader);
        } else {
            try reader.skipValue();
        }
    }

    fn handleCacheField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "read")) {
            self.counts.cache_read = try provider.jsonParseU64Value(allocator, reader);
        } else if (std.mem.eql(u8, key, "write")) {
            self.counts.cache_write = try provider.jsonParseU64Value(allocator, reader);
        } else {
            try reader.skipValue();
        }
    }

    fn handleModelField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (!std.mem.eql(u8, key, "modelID")) {
            try reader.skipValue();
            return;
        }
        var token = try provider.jsonReadStringToken(allocator, reader);
        defer token.deinit(allocator);
        try self.captureModel(token.view());
    }

    fn emit(self: *MessageRecord) !void {
        if (!self.role_assistant) return;
        if (!self.tokens_present) return;
        const millis = self.timestamp_ms orelse return;
        const model_name = self.model_name orelse return;

        const event_model = model_name;
        self.model_name = null;

        const iso = try formatUnixMillis(self.allocator, millis);
        defer self.allocator.free(iso);

        const timestamp_info = (try provider.timestampFromSlice(
            self.allocator,
            iso,
            self.timezone_offset_minutes,
        )) orelse return;

        const usage = self.counts.toUsage();
        if (!provider.shouldEmitUsage(usage)) return;

        const event = model.TokenUsageEvent{
            .session_id = self.session_label,
            .timestamp = timestamp_info.text,
            .local_iso_date = timestamp_info.local_iso_date,
            .model = event_model,
            .usage = usage,
            .is_fallback = false,
            .display_input_tokens = self.ctx.computeDisplayInput(usage),
        };
        try self.sink.emit(event);
    }
};

fn parseSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    _ = deduper;
    var session_label = session_id;
    var session_label_overridden = false;

    const io = runtime.io;

    const resolved_id = determineSessionIdentifier(allocator, ctx, runtime, file_path) catch |err| {
        ctx.logWarning(file_path, "failed to read opencode session metadata", err);
        return;
    };
    defer allocator.free(resolved_id);
    provider.overrideSessionLabelFromSlice(allocator, &session_label, &session_label_overridden, resolved_id);

    const message_dir = buildMessageDirPath(allocator, file_path, resolved_id) catch |err| {
        ctx.logWarning(file_path, "unable to locate opencode messages", err);
        return;
    };
    defer allocator.free(message_dir);

    var dir = std.fs.openDirAbsolute(message_dir, .{ .iterate = true }) catch |err| {
        ctx.logWarning(message_dir, "unable to open opencode message dir", err);
        return;
    };
    defer dir.close();

    var files = std.ArrayList([]u8).empty;
    defer {
        for (files.items) |path| allocator.free(path);
        files.deinit(allocator);
    }

    var iterator = dir.iterate();
    while (try iterator.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".json")) continue;
        const absolute = std.fs.path.join(allocator, &.{ message_dir, entry.name }) catch |err| {
            ctx.logWarning(message_dir, "unable to build opencode message path", err);
            continue;
        };
        try files.append(allocator, absolute);
    }

    std.sort.pdq([]u8, files.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (files.items) |message_path| {
        parseMessageFile(allocator, ctx, io, session_label, message_path, timezone_offset_minutes, sink) catch |err| {
            ctx.logWarning(message_path, "failed to parse opencode message", err);
        };
    }
}

fn determineSessionIdentifier(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    file_path: []const u8,
) ![]u8 {
    if (readSessionIdentifier(allocator, ctx, runtime, file_path) catch null) |value| return value;
    const base = std.fs.path.basename(file_path);
    if (std.mem.endsWith(u8, base, ".json")) {
        return allocator.dupe(u8, base[0 .. base.len - 5]);
    }
    return allocator.dupe(u8, base);
}

fn readSessionIdentifier(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    file_path: []const u8,
) !?[]u8 {
    var identifier: ?[]u8 = null;
    const HandleContext = struct {
        fn root(id_ptr: *?[]u8, scratch: std.mem.Allocator, reader: *std.json.Reader) !void {
            try provider.jsonWalkObject(scratch, reader, id_ptr, field);
        }

        fn field(ptr: *?[]u8, alloc: std.mem.Allocator, r: *std.json.Reader, key: []const u8) !void {
            if (!std.mem.eql(u8, key, "id")) {
                try r.skipValue();
                return;
            }
            if (ptr.* != null) {
                try r.skipValue();
                return;
            }
            var token = try provider.jsonReadStringToken(alloc, r);
            defer token.deinit(alloc);
            const trimmed = std.mem.trim(u8, token.view(), " \r\n\t");
            if (trimmed.len == 0) return;
            ptr.* = try alloc.dupe(u8, trimmed);
        }
    };

    try provider.withJsonObjectReader(
        allocator,
        ctx,
        runtime,
        file_path,
        .{ .max_bytes = 1024 * 64 },
        &identifier,
        HandleContext.root,
    );
    return identifier;
}

fn buildMessageDirPath(
    allocator: std.mem.Allocator,
    session_file_path: []const u8,
    session_identifier: []const u8,
) ![]u8 {
    const marker_unix = "/storage/session/";
    if (std.mem.findLast(u8, session_file_path, marker_unix)) |idx| {
        const prefix = session_file_path[0..idx];
        return std.fmt.allocPrint(allocator, "{s}/storage/message/{s}", .{ prefix, session_identifier });
    }
    const marker_win = "\\storage\\session\\";
    if (std.mem.findLast(u8, session_file_path, marker_win)) |idx| {
        const prefix = session_file_path[0..idx];
        return std.fmt.allocPrint(allocator, "{s}\\storage\\message\\{s}", .{ prefix, session_identifier });
    }
    return error.InvalidSessionPath;
}

fn parseMessageFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    io: std.Io,
    session_label: []const u8,
    file_path: []const u8,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    const file = try std.fs.openFileAbsolute(file_path, .{});
    defer file.close();

    var reader_buffer: [4 * 1024]u8 = undefined;
    var file_reader = file.readerStreaming(io, reader_buffer[0..]);
    var json_reader = std.json.Reader.init(allocator, &file_reader.interface);
    defer json_reader.deinit();

    if ((try json_reader.next()) != .object_begin) return;

    var record = MessageRecord{
        .allocator = allocator,
        .ctx = ctx,
        .sink = sink,
        .session_label = session_label,
        .timezone_offset_minutes = timezone_offset_minutes,
    };
    defer if (record.model_name) |name| allocator.free(name);

    try provider.jsonWalkObject(allocator, &json_reader, &record, MessageRecord.handleField);
    try record.emit();
}

fn formatUnixMillis(allocator: std.mem.Allocator, millis: u64) ![]u8 {
    const secs = millis / 1000;
    const ms: u16 = @intCast(millis % 1000);
    const epoch = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = epoch.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch.getDaySeconds();
    return std.fmt.allocPrint(
        allocator,
        "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z",
        .{
            year_day.year,
            month_day.month.numeric(),
            @as(u8, month_day.day_index) + 1,
            day_seconds.getHoursIntoDay(),
            day_seconds.getMinutesIntoHour(),
            day_seconds.getSecondsIntoMinute(),
            ms,
        },
    );
}

fn parseMessageFileTestWrapper(
    allocator: std.mem.Allocator,
    session_path: []const u8,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const io = io_single.io();

    const ctx = provider.ParseContext{
        .provider_name = "opencode-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };
    const runtime = provider.ParseRuntime{ .io = io };

    const absolute_path = try std.fs.cwd().realpathAlloc(allocator, session_path);
    defer allocator.free(absolute_path);
    try parseSessionFile(
        allocator,
        &ctx,
        &runtime,
        "fixture-session",
        absolute_path,
        null,
        timezone_offset_minutes,
        sink,
    );
}

test "opencode parser emits assistant usage events" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    try parseMessageFileTestWrapper(
        worker_allocator,
        "fixtures/opencode/storage/session/demo_project/ses_fixture_one.json",
        0,
        sink,
    );

    try std.testing.expectEqual(@as(usize, 2), events.items.len);
    const first = events.items[0];
    try std.testing.expectEqualStrings("ses_fixture_one", first.session_id);
    try std.testing.expectEqualStrings("grok-code", first.model);
    try std.testing.expectEqual(@as(u64, 1200), first.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 300), first.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 100), first.usage.cache_creation_input_tokens);
    try std.testing.expectEqual(@as(u64, 200), first.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 50), first.usage.reasoning_output_tokens);

    const second = events.items[1];
    try std.testing.expectEqualStrings("ses_fixture_one", second.session_id);
    try std.testing.expectEqualStrings("scout-preview", second.model);
    try std.testing.expectEqual(@as(u64, 400), second.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 0), second.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 80), second.usage.output_tokens);
}

test "opencode message parser handles large documents" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const large_len: usize = 700 * 1024;
    const large_payload = try worker_allocator.alloc(u8, large_len);
    @memset(large_payload, 'a');

    var msg_file = try tmp.dir.createFile("msg_large.json", .{});
    defer msg_file.close();

    try msg_file.writeAll("{\"role\":\"assistant\",\"time\":{\"completed\":1700000000000},\"tokens\":{\"input\":1,\"output\":2},\"modelID\":\"big-model\",\"content\":\"");
    try msg_file.writeAll(large_payload);
    try msg_file.writeAll("\"}");

    const msg_path = try tmp.dir.realpathAlloc(worker_allocator, "msg_large.json");
    defer worker_allocator.free(msg_path);

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    const ctx = provider.ParseContext{
        .provider_name = "opencode-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const io = io_single.io();

    try parseMessageFile(worker_allocator, &ctx, io, "fixture-session", msg_path, 0, sink);

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("fixture-session", event.session_id);
    try std.testing.expectEqualStrings("big-model", event.model);
    try std.testing.expectEqual(@as(u64, 1), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 2), event.usage.output_tokens);
}
