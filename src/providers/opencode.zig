const std = @import("std");
const ascii = std.ascii;

const model = @import("../model.zig");
const RawUsage = model.RawTokenUsage;
const provider = @import("provider.zig");
const MessageDeduper = provider.MessageDeduper;
const log = std.log.scoped(.provider_opencode);

const ProviderExports = provider.makeProvider(.{
    .scope = .opencode,
    .sessions_dir_suffix = "/.local/share/opencode/storage/session",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".json",
    .extra_session_file_suffixes = &.{"/.local/share/opencode/opencode.db"},
    .cached_counts_overlap_input = false,
    .requires_deduper = true,
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
    sink: provider.EventSink,
    session_label: []const u8,
    timezone_offset_minutes: i32,
    io: std.Io,
    deduper: ?*MessageDeduper = null,

    role_assistant: bool = false,
    timestamp_ms: ?u64 = null,
    tokens_present: bool = false,
    counts: TokenCounts = .{},
    message_id: ?[]const u8 = null,
    model_name: ?[]const u8 = null,

    fn handleField(self: *MessageRecord, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
        if (std.mem.eql(u8, key, "role")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            self.role_assistant = ascii.eqlIgnoreCase(token.view(), "assistant");
            return;
        }
        if (std.mem.eql(u8, key, "id")) {
            var token = try provider.jsonReadStringToken(allocator, reader);
            defer token.deinit(allocator);
            try self.captureMessageId(token.view());
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
        try self.captureTrimmedOnce(&self.model_name, raw);
    }

    fn captureMessageId(self: *MessageRecord, raw: []const u8) !void {
        try self.captureTrimmedOnce(&self.message_id, raw);
    }

    fn captureTrimmedOnce(self: *MessageRecord, slot: *?[]const u8, raw: []const u8) !void {
        if (slot.* != null) return;
        const trimmed = std.mem.trim(u8, raw, " \r\n\t");
        if (trimmed.len == 0) return;
        slot.* = try self.allocator.dupe(u8, trimmed);
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
            .display_input_tokens = provider.computeDisplayInput(usage),
        };

        if (self.deduper) |deduper| {
            if (self.message_id) |message_id| {
                const key = messageDeduperKey(self.session_label, message_id);
                if (!(try deduper.mark(self.io, key))) return;
            }
        }

        try self.sink.emit(self.io, event);
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
    if (std.mem.endsWith(u8, file_path, ".db")) {
        try parseSqliteSessionFile(
            allocator,
            ctx,
            runtime,
            file_path,
            deduper,
            timezone_offset_minutes,
            sink,
        );
        return;
    }

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

    var dir = std.Io.Dir.openDirAbsolute(io, message_dir, .{ .iterate = true }) catch |err| {
        ctx.logWarning(message_dir, "unable to open opencode message dir", err);
        return;
    };
    defer dir.close(io);

    var files = std.ArrayList([]u8).empty;
    defer {
        for (files.items) |path| allocator.free(path);
        files.deinit(allocator);
    }

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
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
        parseMessageFile(allocator, io, session_label, message_path, deduper, timezone_offset_minutes, sink) catch |err| {
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
    io: std.Io,
    session_label: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    const file = try std.Io.Dir.openFileAbsolute(io, file_path, .{});
    defer file.close(io);

    var reader_buffer: [4 * 1024]u8 = undefined;
    var file_reader = file.readerStreaming(io, reader_buffer[0..]);
    var json_reader = std.json.Reader.init(allocator, &file_reader.interface);
    defer json_reader.deinit();

    if ((try json_reader.next()) != .object_begin) return;

    var record = MessageRecord{
        .allocator = allocator,
        .sink = sink,
        .session_label = session_label,
        .timezone_offset_minutes = timezone_offset_minutes,
        .io = io,
        .deduper = deduper,
    };
    defer if (record.model_name) |name| allocator.free(name);
    defer if (record.message_id) |id| allocator.free(id);

    try provider.jsonWalkObject(allocator, &json_reader, &record, MessageRecord.handleField);
    try record.emit();
}

fn parseSqliteSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    db_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    const query =
        \\SELECT
        \\  id AS message_id,
        \\  session_id,
        \\  COALESCE(json_extract(data, '$.time.completed'), json_extract(data, '$.time.created')) AS timestamp_ms,
        \\  COALESCE(json_extract(data, '$.modelID'), json_extract(data, '$.model.modelID')) AS model_name,
        \\  CAST(COALESCE(json_extract(data, '$.tokens.input'), 0) AS INTEGER) AS input_tokens,
        \\  CAST(COALESCE(json_extract(data, '$.tokens.output'), 0) AS INTEGER) AS output_tokens,
        \\  CAST(COALESCE(json_extract(data, '$.tokens.reasoning'), 0) AS INTEGER) AS reasoning_tokens,
        \\  CAST(COALESCE(json_extract(data, '$.tokens.cache.read'), 0) AS INTEGER) AS cache_read_tokens,
        \\  CAST(COALESCE(json_extract(data, '$.tokens.cache.write'), 0) AS INTEGER) AS cache_write_tokens
        \\FROM message
        \\WHERE json_extract(data, '$.role') = 'assistant'
        \\  AND json_extract(data, '$.tokens') IS NOT NULL
        \\ORDER BY COALESCE(json_extract(data, '$.time.completed'), json_extract(data, '$.time.created'))
    ;

    const json_rows = runSqliteQuery(allocator, runtime, db_path, query) catch |err| {
        if (err == error.SqliteCommandMissing) {
            ctx.logWarning(db_path, "sqlite3 command not found", err);
            return;
        }
        return err;
    };
    defer allocator.free(json_rows);

    try parseSqliteRowsJson(allocator, runtime.io, timezone_offset_minutes, deduper, sink, json_rows);
}

fn parseSqliteRowsJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    timezone_offset_minutes: i32,
    deduper: ?*MessageDeduper,
    sink: provider.EventSink,
    json_payload: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_payload, .{});
    defer parsed.deinit();

    switch (parsed.value) {
        .array => |rows| {
            for (rows.items) |row_value| {
                try parseSqliteRow(allocator, io, timezone_offset_minutes, deduper, sink, row_value);
            }
        },
        else => return error.InvalidJson,
    }
}

fn parseSqliteRow(
    allocator: std.mem.Allocator,
    io: std.Io,
    timezone_offset_minutes: i32,
    deduper: ?*MessageDeduper,
    sink: provider.EventSink,
    row_value: std.json.Value,
) !void {
    const row = switch (row_value) {
        .object => |obj| obj,
        else => return,
    };

    const session_label = readSqliteString(row, "session_id") orelse return;
    const message_id = readSqliteString(row, "message_id") orelse return;
    const model_name = readSqliteString(row, "model_name") orelse return;
    const timestamp_ms = readSqliteU64(row, "timestamp_ms") orelse return;

    const counts = TokenCounts{
        .input = readSqliteU64(row, "input_tokens") orelse 0,
        .output = readSqliteU64(row, "output_tokens") orelse 0,
        .reasoning = readSqliteU64(row, "reasoning_tokens") orelse 0,
        .cache_read = readSqliteU64(row, "cache_read_tokens") orelse 0,
        .cache_write = readSqliteU64(row, "cache_write_tokens") orelse 0,
    };

    const usage = counts.toUsage();
    if (!provider.shouldEmitUsage(usage)) return;

    const iso = try formatUnixMillis(allocator, timestamp_ms);
    defer allocator.free(iso);
    const timestamp_info = (try provider.timestampFromSlice(
        allocator,
        iso,
        timezone_offset_minutes,
    )) orelse return;

    const event = model.TokenUsageEvent{
        .session_id = session_label,
        .timestamp = timestamp_info.text,
        .local_iso_date = timestamp_info.local_iso_date,
        .model = model_name,
        .usage = usage,
        .is_fallback = false,
        .display_input_tokens = provider.computeDisplayInput(usage),
    };

    if (deduper) |seen| {
        const dedupe_key = messageDeduperKey(session_label, message_id);
        if (!(try seen.mark(io, dedupe_key))) return;
    }

    try sink.emit(io, event);
}

fn readSqliteString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |s| s,
        .number_string => |s| s,
        else => null,
    };
}

fn readSqliteU64(obj: std.json.ObjectMap, key: []const u8) ?u64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |v| if (v >= 0) @as(u64, @intCast(v)) else 0,
        .float => |v| if (v >= 0) @as(u64, @intFromFloat(std.math.floor(v))) else 0,
        .number_string => |s| parseU64Slice(s),
        .string => |s| parseU64Slice(s),
        else => null,
    };
}

fn parseU64Slice(raw: []const u8) ?u64 {
    const trimmed = std.mem.trim(u8, raw, " \r\n\t");
    if (trimmed.len == 0) return null;
    return std.fmt.parseInt(u64, trimmed, 10) catch null;
}

fn runSqliteQuery(
    allocator: std.mem.Allocator,
    runtime: *const provider.ParseRuntime,
    db_path: []const u8,
    query: []const u8,
) ![]u8 {
    var argv = [_][]const u8{ "sqlite3", "-json", db_path, query };
    var result = std.process.run(allocator, runtime.io, .{
        .argv = argv[0..],
        .stdout_limit = .limited(64 * 1024 * 1024),
        .stderr_limit = .limited(64 * 1024 * 1024),
    }) catch |err| {
        if (err == error.FileNotFound) return error.SqliteCommandMissing;
        return err;
    };
    defer allocator.free(result.stderr);

    const exit_code: u8 = switch (result.term) {
        .exited => |code| code,
        else => 255,
    };
    if (exit_code != 0) {
        const stderr = std.mem.trim(u8, result.stderr, " \r\n\t");
        if (stderr.len > 0) {
            log.warn("sqlite3 query failed for '{s}' (exit {d}): {s}", .{ db_path, exit_code, stderr });
        } else {
            log.warn("sqlite3 query failed for '{s}' (exit {d})", .{ db_path, exit_code });
        }
        allocator.free(result.stdout);
        return error.SqliteFailed;
    }

    return result.stdout;
}

fn messageDeduperKey(session_id: []const u8, message_id: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(session_id);
    hasher.update(&[_]u8{0});
    hasher.update(message_id);
    return hasher.final();
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
    const io = std.testing.io;

    const ctx = provider.ParseContext{
        .provider_name = "opencode-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };
    const runtime = provider.ParseRuntime{ .io = io };

    const absolute_path = try std.Io.Dir.cwd().realPathFileAlloc(io, session_path, allocator);
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

fn sqliteRowsFixtureJson() []const u8 {
    return
        \\[
        \\  {
        \\    "message_id": "msg_builder",
        \\    "session_id": "ses_fixture_one",
        \\    "timestamp_ms": 1759302005000,
        \\    "model_name": "grok-code",
        \\    "input_tokens": 1200,
        \\    "output_tokens": 200,
        \\    "reasoning_tokens": 50,
        \\    "cache_read_tokens": 300,
        \\    "cache_write_tokens": 100
        \\  },
        \\  {
        \\    "message_id": "msg_nested",
        \\    "session_id": "ses_fixture_one",
        \\    "timestamp_ms": 1759302600000,
        \\    "model_name": "scout-preview",
        \\    "input_tokens": 400,
        \\    "output_tokens": 80,
        \\    "reasoning_tokens": 0,
        \\    "cache_read_tokens": 0,
        \\    "cache_write_tokens": 0
        \\  }
        \\]
    ;
}

fn sqliteRowsSameMessageZeroThenNonZeroJson() []const u8 {
    return
        \\[
        \\  {
        \\    "message_id": "msg_same",
        \\    "session_id": "ses_fixture_one",
        \\    "timestamp_ms": 1759302005000,
        \\    "model_name": "grok-code",
        \\    "input_tokens": 0,
        \\    "output_tokens": 0,
        \\    "reasoning_tokens": 0,
        \\    "cache_read_tokens": 0,
        \\    "cache_write_tokens": 0
        \\  },
        \\  {
        \\    "message_id": "msg_same",
        \\    "session_id": "ses_fixture_one",
        \\    "timestamp_ms": 1759302600000,
        \\    "model_name": "grok-code",
        \\    "input_tokens": 10,
        \\    "output_tokens": 2,
        \\    "reasoning_tokens": 1,
        \\    "cache_read_tokens": 3,
        \\    "cache_write_tokens": 0
        \\  }
        \\]
    ;
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

test "opencode sqlite parser emits assistant usage events" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    try parseSqliteRowsJson(
        worker_allocator,
        std.testing.io,
        0,
        null,
        sink,
        sqliteRowsFixtureJson(),
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

test "opencode sqlite parser dedupes repeated rows" {
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

    try parseSqliteRowsJson(
        worker_allocator,
        std.testing.io,
        0,
        &deduper,
        sink,
        sqliteRowsFixtureJson(),
    );
    try parseSqliteRowsJson(
        worker_allocator,
        std.testing.io,
        0,
        &deduper,
        sink,
        sqliteRowsFixtureJson(),
    );

    try std.testing.expectEqual(@as(usize, 2), events.items.len);
}

test "opencode sqlite deduper keeps later non-zero usage event" {
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

    try parseSqliteRowsJson(
        worker_allocator,
        std.testing.io,
        0,
        &deduper,
        sink,
        sqliteRowsSameMessageZeroThenNonZeroJson(),
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("ses_fixture_one", event.session_id);
    try std.testing.expectEqualStrings("grok-code", event.model);
    try std.testing.expectEqual(@as(u64, 10), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 2), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 1), event.usage.reasoning_output_tokens);
    try std.testing.expectEqual(@as(u64, 3), event.usage.cached_input_tokens);
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

    const io = std.testing.io;

    var msg_file = try tmp.dir.createFile(io, "msg_large.json", .{});
    defer msg_file.close(io);

    try msg_file.writeStreamingAll(io, "{\"role\":\"assistant\",\"time\":{\"completed\":1700000000000},\"tokens\":{\"input\":1,\"output\":2},\"modelID\":\"big-model\",\"content\":\"");
    try msg_file.writeStreamingAll(io, large_payload);
    try msg_file.writeStreamingAll(io, "\"}");

    const msg_path = try tmp.dir.realPathFileAlloc(io, "msg_large.json", worker_allocator);
    defer worker_allocator.free(msg_path);

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    try parseMessageFile(worker_allocator, io, "fixture-session", msg_path, null, 0, sink);

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("fixture-session", event.session_id);
    try std.testing.expectEqualStrings("big-model", event.model);
    try std.testing.expectEqual(@as(u64, 1), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 2), event.usage.output_tokens);
}
