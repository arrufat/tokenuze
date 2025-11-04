const std = @import("std");
const Model = @import("../model.zig");
const timeutil = @import("../time.zig");

const LEGACY_FALLBACK_MODEL = "gpt-5";
const JSON_EXT = ".jsonl";

const FALLBACK_PRICING = [_]struct {
    name: []const u8,
    pricing: Model.ModelPricing,
}{
    .{ .name = "gpt-5", .pricing = .{ .input_cost_per_m = 1.25, .cached_input_cost_per_m = 0.125, .output_cost_per_m = 10.0 } },
    .{ .name = "gpt-5-codex", .pricing = .{ .input_cost_per_m = 1.25, .cached_input_cost_per_m = 0.125, .output_cost_per_m = 10.0 } },
};

const RawUsage = Model.RawTokenUsage;
const TokenString = Model.TokenBuffer;

const PayloadResult = struct {
    payload_type: ?Model.TokenBuffer = null,
    model: ?Model.TokenBuffer = null,
    last_usage: ?RawUsage = null,
    total_usage: ?RawUsage = null,

    fn deinit(self: *PayloadResult, allocator: std.mem.Allocator) void {
        if (self.payload_type) |*tok| tok.release(allocator);
        if (self.model) |*tok| tok.release(allocator);
        self.* = .{};
    }
};

const ParseError = error{
    UnexpectedToken,
    InvalidNumber,
};

const ScannerAllocError = std.json.Scanner.AllocError;
const ScannerSkipError = std.json.Scanner.SkipError;
const ScannerNextError = std.json.Scanner.NextError;
const ScannerPeekError = std.json.Scanner.PeekError;

const ParserError = ParseError || ScannerAllocError || ScannerSkipError || ScannerNextError || ScannerPeekError;

pub fn collect(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
    pricing: *Model.PricingMap,
    progress: ?std.Progress.Node,
) !void {
    var total_timer = try std.time.Timer.start();

    if (progress) |node| std.Progress.Node.setEstimatedTotalItems(node, 2);

    var events_progress: ?std.Progress.Node = null;
    if (progress) |node| {
        events_progress = std.Progress.Node.start(node, "scan sessions", 0);
    }
    var events_timer = try std.time.Timer.start();
    const before_events = events.items.len;
    try collectEvents(allocator, arena, events, events_progress);
    const after_events = events.items.len;
    if (events_progress) |node| std.Progress.Node.end(node);
    std.log.info(
        "codex.collectEvents produced {d} new events in {d:.2}ms",
        .{ after_events - before_events, nsToMs(events_timer.read()) },
    );

    var pricing_progress: ?std.Progress.Node = null;
    if (progress) |node| {
        pricing_progress = std.Progress.Node.start(node, "pricing", 0);
    }
    var pricing_timer = try std.time.Timer.start();
    const before_pricing = pricing.count();
    try loadPricing(arena, allocator, pricing);
    const after_pricing = pricing.count();
    if (pricing_progress) |node| std.Progress.Node.end(node);
    std.log.info(
        "codex.loadPricing added {d} pricing models (total={d}) in {d:.2}ms",
        .{ after_pricing - before_pricing, after_pricing, nsToMs(pricing_timer.read()) },
    );

    std.log.info(
        "codex.collect completed in {d:.2}ms",
        .{nsToMs(total_timer.read())},
    );
}

fn collectEvents(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
    progress: ?std.Progress.Node,
) !void {
    const SessionJob = struct {
        path: []u8,
        events_added: usize = 0,
    };

    const SharedContext = struct {
        allocator: std.mem.Allocator,
        arena: std.mem.Allocator,
        events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
        events_mutex: *std.Thread.Mutex,
        progress: ?std.Progress.Node,
    };

    const ProcessFn = struct {
        fn run(shared: *SharedContext, job: *SessionJob) void {
            defer if (shared.progress) |node| std.Progress.Node.completeOne(node);

            var local_events = std.ArrayListUnmanaged(Model.TokenUsageEvent){};
            defer local_events.deinit(shared.allocator);

            const basename = std.fs.path.basename(job.path);
            if (basename.len <= JSON_EXT.len or !std.mem.endsWith(u8, basename, JSON_EXT)) return;
            const session_id_slice = basename[0 .. basename.len - JSON_EXT.len];

            const maybe_session_id = duplicateNonEmpty(shared.arena, session_id_slice) catch {
                return;
            };
            const session_id = maybe_session_id orelse return;

            parseSessionFile(shared.allocator, shared.arena, session_id, job.path, &local_events) catch {
                return;
            };

            if (local_events.items.len == 0) return;

            shared.events_mutex.lock();
            defer shared.events_mutex.unlock();

            shared.events.ensureTotalCapacity(shared.allocator, shared.events.items.len + local_events.items.len) catch {
                return;
            };
            shared.events.appendSliceAssumeCapacity(local_events.items);

            job.events_added = local_events.items.len;
        }
    };

    var timer = try std.time.Timer.start();

    const sessions_dir = resolveSessionsDir(allocator) catch |err| {
        std.log.info("codex.collectEvents: skipping, unable to resolve sessions dir ({s})", .{@errorName(err)});
        return;
    };
    defer allocator.free(sessions_dir);

    var root_dir = std.fs.openDirAbsolute(sessions_dir, .{ .iterate = true }) catch |err| {
        std.log.info(
            "codex.collectEvents: skipping, unable to open sessions dir '{s}' ({s})",
            .{ sessions_dir, @errorName(err) },
        );
        return;
    };
    defer root_dir.close();

    var walker = try root_dir.walk(allocator);
    defer walker.deinit();

    var jobs = std.ArrayListUnmanaged(SessionJob){};
    defer {
        for (jobs.items) |job| allocator.free(job.path);
        jobs.deinit(allocator);
    }

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        const relative_path = std.mem.sliceTo(entry.path, 0);
        if (!std.mem.endsWith(u8, relative_path, JSON_EXT)) continue;

        const absolute_path = try std.fs.path.join(allocator, &.{ sessions_dir, relative_path });
        try jobs.append(allocator, .{ .path = absolute_path });
    }

    const files_processed = jobs.items.len;

    if (progress) |node| {
        std.Progress.Node.setEstimatedTotalItems(node, files_processed);
        std.Progress.Node.setCompletedItems(node, 0);
    }

    if (files_processed == 0) {
        std.log.info(
            "codex.collectEvents: scanned 0 files, added 0 events in {d:.2}ms",
            .{nsToMs(timer.read())},
        );
        return;
    }

    var events_mutex = std.Thread.Mutex{};

    var thread_safe_arena = std.heap.ThreadSafeAllocator{ .child_allocator = arena };
    var shared = SharedContext{
        .allocator = allocator,
        .arena = thread_safe_arena.allocator(),
        .events = events,
        .events_mutex = &events_mutex,
        .progress = progress,
    };

    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    const io = threaded.io();
    var group = std.Io.Group.init;

    for (jobs.items) |*job| {
        group.async(io, ProcessFn.run, .{ &shared, job });
    }
    group.wait(io);

    var events_added: usize = 0;
    for (jobs.items) |job| {
        events_added += job.events_added;
    }

    std.log.info(
        "codex.collectEvents: scanned {d} files, added {d} events in {d:.2}ms",
        .{ files_processed, events_added, nsToMs(timer.read()) },
    );
}

fn resolveSessionsDir(allocator: std.mem.Allocator) ![]u8 {
    const home = try std.process.getEnvVarOwned(allocator, "HOME");
    defer allocator.free(home);
    return std.fmt.allocPrint(allocator, "{s}/.codex/sessions", .{home});
}

fn parseSessionFile(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    session_id: []const u8,
    file_path: []const u8,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
) !void {
    var previous_totals: ?RawUsage = null;
    var current_model: ?[]const u8 = null;
    var current_model_is_fallback = false;
    const max_session_size: usize = 128 * 1024 * 1024;
    const file_bytes = std.fs.cwd().readFileAlloc(file_path, allocator, std.Io.Limit.limited(max_session_size)) catch {
        return;
    };
    defer allocator.free(file_bytes);

    var lines = std.mem.splitScalar(u8, file_bytes, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0) continue;

        var scanner = std.json.Scanner.initCompleteInput(allocator, trimmed);
        defer scanner.deinit();

        const start_token = scanner.next() catch {
            continue;
        };
        if (start_token != .object_begin) continue;

        var payload_result = PayloadResult{};
        defer payload_result.deinit(allocator);
        var timestamp_token: ?Model.TokenBuffer = null;
        defer if (timestamp_token) |*tok| tok.release(allocator);
        var is_turn_context = false;
        var is_event_msg = false;
        var parse_failed = false;

        while (!parse_failed) {
            const key_token = scanner.nextAlloc(allocator, .alloc_if_needed) catch {
                parse_failed = true;
                break;
            };

            switch (key_token) {
                .object_end => break,
                .string => |slice| {
                    var key = Model.TokenBuffer{ .slice = slice, .owned = null };
                    defer key.release(allocator);
                    parseObjectField(
                        allocator,
                        &scanner,
                        key.slice,
                        &payload_result,
                        &timestamp_token,
                        &is_turn_context,
                        &is_event_msg,
                    ) catch {
                        parse_failed = true;
                    };
                },
                .allocated_string => |buf| {
                    var key = Model.TokenBuffer{ .slice = buf, .owned = buf };
                    defer key.release(allocator);
                    parseObjectField(
                        allocator,
                        &scanner,
                        key.slice,
                        &payload_result,
                        &timestamp_token,
                        &is_turn_context,
                        &is_event_msg,
                    ) catch {
                        parse_failed = true;
                    };
                },
                else => {
                    parse_failed = true;
                },
            }
        }

        if (parse_failed) continue;

        _ = scanner.next() catch {};

        if (timestamp_token == null) continue;

        if (is_turn_context) {
            if (payload_result.model) |token| {
                var model_token = token;
                payload_result.model = null;
                if (model_token.slice.len != 0) {
                    const duplicated = duplicateNonEmpty(arena, model_token.slice) catch null;
                    if (duplicated) |model_copy| {
                        current_model = model_copy;
                        current_model_is_fallback = false;
                    }
                }
                model_token.release(allocator);
            }
            continue;
        }

        if (!is_event_msg) continue;

        var payload_type_is_token_count = false;
        if (payload_result.payload_type) |token| {
            payload_type_is_token_count = std.mem.eql(u8, token.slice, "token_count");
        }
        if (!payload_type_is_token_count) continue;

        var raw_timestamp = timestamp_token.?;
        timestamp_token = null;
        const timestamp_copy = try duplicateNonEmpty(arena, raw_timestamp.slice) orelse {
            raw_timestamp.release(allocator);
            continue;
        };
        raw_timestamp.release(allocator);
        const iso_date = timeutil.localIsoDateFromTimestamp(timestamp_copy) catch {
            continue;
        };

        var delta_usage: ?Model.TokenUsage = null;
        if (payload_result.last_usage) |last_usage| {
            delta_usage = Model.TokenUsage.fromRaw(last_usage);
        } else if (payload_result.total_usage) |total_usage| {
            delta_usage = Model.TokenUsage.deltaFrom(total_usage, previous_totals);
        }
        if (payload_result.total_usage) |total_usage| {
            previous_totals = total_usage;
        }

        if (delta_usage == null) continue;

        const delta = delta_usage.?;
        if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
            continue;
        }

        var extracted_model: ?[]const u8 = null;
        if (payload_result.model) |token| {
            var model_token = token;
            payload_result.model = null;
            if (model_token.slice.len != 0) {
                extracted_model = try duplicateNonEmpty(arena, model_token.slice);
                if (extracted_model == null) extracted_model = current_model;
            }
            model_token.release(allocator);
        }

        var model_name = extracted_model;
        var is_fallback = false;
        if (model_name == null) {
            if (current_model) |known| {
                model_name = known;
                is_fallback = current_model_is_fallback;
            } else {
                model_name = LEGACY_FALLBACK_MODEL;
                is_fallback = true;
                current_model = model_name;
                current_model_is_fallback = true;
            }
        } else {
            current_model = model_name;
            current_model_is_fallback = false;
        }

        if (model_name == null) {
            continue;
        }
        if (std.mem.eql(u8, model_name.?, LEGACY_FALLBACK_MODEL) and extracted_model == null) {
            is_fallback = true;
            current_model_is_fallback = true;
        }

        const event = Model.TokenUsageEvent{
            .session_id = session_id,
            .timestamp = timestamp_copy,
            .local_iso_date = iso_date,
            .model = model_name.?,
            .usage = delta,
            .is_fallback = is_fallback,
        };
        try events.append(allocator, event);
    }
}

fn parseObjectField(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
    payload_result: *PayloadResult,
    timestamp_token: *?TokenString,
    is_turn_context: *bool,
    is_event_msg: *bool,
) ParserError!void {
    if (std.mem.eql(u8, key, "type")) {
        var value = try readStringToken(scanner, allocator);
        defer value.release(allocator);
        if (std.mem.eql(u8, value.slice, "turn_context")) {
            is_turn_context.* = true;
            is_event_msg.* = false;
        } else if (std.mem.eql(u8, value.slice, "event_msg")) {
            is_event_msg.* = true;
            is_turn_context.* = false;
        }
        return;
    }

    if (std.mem.eql(u8, key, "timestamp")) {
        if (timestamp_token.*) |*existing| existing.release(allocator);
        timestamp_token.* = try readStringToken(scanner, allocator);
        return;
    }

    if (std.mem.eql(u8, key, "payload")) {
        try parsePayload(allocator, scanner, payload_result);
        return;
    }

    try scanner.skipValue();
}

fn readStringToken(scanner: *std.json.Scanner, allocator: std.mem.Allocator) ParserError!TokenString {
    const token = try scanner.nextAlloc(allocator, .alloc_if_needed);
    return switch (token) {
        .string => |slice| TokenString{ .slice = slice },
        .allocated_string => |buf| TokenString{ .slice = buf, .owned = buf },
        else => ParseError.UnexpectedToken,
    };
}

fn readOptionalStringToken(scanner: *std.json.Scanner, allocator: std.mem.Allocator) ParserError!?TokenString {
    const peek = try scanner.peekNextTokenType();
    switch (peek) {
        .null => {
            _ = try scanner.next();
            return null;
        },
        .string => return try readStringToken(scanner, allocator),
        else => return ParseError.UnexpectedToken,
    }
}

fn readNumberToken(scanner: *std.json.Scanner, allocator: std.mem.Allocator) ParserError!TokenString {
    const token = try scanner.nextAlloc(allocator, .alloc_if_needed);
    return switch (token) {
        .number => |slice| TokenString{ .slice = slice },
        .allocated_number => |buf| TokenString{ .slice = buf, .owned = buf },
        else => ParseError.UnexpectedToken,
    };
}

fn replaceToken(dest: *?TokenString, allocator: std.mem.Allocator, token: TokenString) void {
    if (dest.*) |*existing| existing.release(allocator);
    dest.* = token;
}

fn captureModelToken(dest: *?TokenString, allocator: std.mem.Allocator, token: TokenString) void {
    if (token.slice.len == 0) {
        var tmp = token;
        tmp.release(allocator);
        return;
    }
    if (dest.* == null) {
        dest.* = token;
    } else {
        var tmp = token;
        tmp.release(allocator);
    }
}

fn isModelKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "model") or std.mem.eql(u8, key, "model_name");
}

fn parsePayload(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    payload_result: *PayloadResult,
) ParserError!void {
    const peek = try scanner.peekNextTokenType();
    if (peek == .null) {
        _ = try scanner.next();
        return;
    }
    if (peek != .object_begin) {
        try scanner.skipValue();
        return;
    }

    _ = try scanner.next(); // consume object_begin

    try walkObject(allocator, scanner, payload_result, handlePayloadField);
}

fn parsePayloadField(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
    payload_result: *PayloadResult,
) ParserError!void {
    if (try parseSharedPayloadField(allocator, scanner, key, payload_result)) return;

    if (std.mem.eql(u8, key, "type")) {
        replaceToken(&payload_result.payload_type, allocator, try readStringToken(scanner, allocator));
        return;
    }

    if (std.mem.eql(u8, key, "info")) {
        try parseInfoObject(allocator, scanner, payload_result);
        return;
    }

    try scanner.skipValue();
}

fn parseInfoObject(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    payload_result: *PayloadResult,
) ParserError!void {
    const peek = try scanner.peekNextTokenType();
    if (peek == .null) {
        _ = try scanner.next();
        return;
    }
    if (peek != .object_begin) {
        try scanner.skipValue();
        return;
    }

    _ = try scanner.next(); // consume object_begin

    try walkObject(allocator, scanner, payload_result, handleInfoField);
}

fn parseInfoField(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
    payload_result: *PayloadResult,
) ParserError!void {
    if (try parseSharedPayloadField(allocator, scanner, key, payload_result)) return;

    if (std.mem.eql(u8, key, "last_token_usage")) {
        payload_result.last_usage = try parseUsageObject(allocator, scanner);
        return;
    }
    if (std.mem.eql(u8, key, "total_token_usage")) {
        payload_result.total_usage = try parseUsageObject(allocator, scanner);
        return;
    }

    try scanner.skipValue();
}

fn parseUsageObject(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
) ParserError!?RawUsage {
    const peek = try scanner.peekNextTokenType();
    if (peek == .null) {
        _ = try scanner.next();
        return null;
    }
    if (peek != .object_begin) {
        try scanner.skipValue();
        return null;
    }

    _ = try scanner.next();

    var accumulator = Model.UsageAccumulator{};

    while (true) {
        const key_token = try scanner.nextAlloc(allocator, .alloc_if_needed);
        switch (key_token) {
            .object_end => break,
            .string => |slice| {
                var key = TokenString{ .slice = slice };
                defer key.release(allocator);
                try parseUsageField(allocator, scanner, key.slice, &accumulator);
            },
            .allocated_string => |buf| {
                var key = TokenString{ .slice = buf, .owned = buf };
                defer key.release(allocator);
                try parseUsageField(allocator, scanner, key.slice, &accumulator);
            },
            else => return ParseError.UnexpectedToken,
        }
    }

    return accumulator.finalize();
}

fn parseUsageField(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
    accumulator: *Model.UsageAccumulator,
) ParserError!void {
    const field = Model.usageFieldForKey(key) orelse {
        try scanner.skipValue();
        return;
    };
    const value = try parseU64Value(scanner, allocator);
    accumulator.applyField(field, value);
}

fn parseModelValue(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    storage: *?TokenString,
) ParserError!void {
    const peek = try scanner.peekNextTokenType();
    switch (peek) {
        .object_begin => {
            _ = try scanner.next();
            while (true) {
                const key_token = try scanner.nextAlloc(allocator, .alloc_if_needed);
                switch (key_token) {
                    .object_end => break,
                    .string => |slice| {
                        var key = TokenString{ .slice = slice };
                        defer key.release(allocator);
                        if (isModelKey(key.slice)) {
                            const maybe_token = try readOptionalStringToken(scanner, allocator);
                            if (maybe_token) |token| captureModelToken(storage, allocator, token);
                        } else {
                            try parseModelValue(allocator, scanner, storage);
                        }
                    },
                    .allocated_string => |buf| {
                        var key = TokenString{ .slice = buf, .owned = buf };
                        defer key.release(allocator);
                        if (isModelKey(key.slice)) {
                            const maybe_token = try readOptionalStringToken(scanner, allocator);
                            if (maybe_token) |token| captureModelToken(storage, allocator, token);
                        } else {
                            try parseModelValue(allocator, scanner, storage);
                        }
                    },
                    else => return ParseError.UnexpectedToken,
                }
            }
        },
        .array_begin => {
            _ = try scanner.next();
            while (true) {
                const next_type = try scanner.peekNextTokenType();
                if (next_type == .array_end) {
                    _ = try scanner.next();
                    break;
                }
                try parseModelValue(allocator, scanner, storage);
            }
        },
        .string => {
            var value = try readStringToken(scanner, allocator);
            value.release(allocator);
        },
        .null => {
            _ = try scanner.next();
        },
        else => try scanner.skipValue(),
    }
}

fn parseSharedPayloadField(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
    payload_result: *PayloadResult,
) ParserError!bool {
    if (isModelKey(key)) {
        const maybe_token = try readOptionalStringToken(scanner, allocator);
        if (maybe_token) |token| {
            captureModelToken(&payload_result.model, allocator, token);
        }
        return true;
    }
    if (std.mem.eql(u8, key, "metadata")) {
        try parseModelValue(allocator, scanner, &payload_result.model);
        return true;
    }
    return false;
}

fn handlePayloadField(
    payload_result: *PayloadResult,
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
) ParserError!void {
    try parsePayloadField(allocator, scanner, key, payload_result);
}

fn handleInfoField(
    payload_result: *PayloadResult,
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    key: []const u8,
) ParserError!void {
    try parseInfoField(allocator, scanner, key, payload_result);
}

fn walkObject(
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Scanner, []const u8) ParserError!void,
) ParserError!void {
    while (true) {
        const key_token = try scanner.nextAlloc(allocator, .alloc_if_needed);
        switch (key_token) {
            .object_end => break,
            .string => |slice| {
                var key = TokenString{ .slice = slice };
                defer key.release(allocator);
                try handler(context, allocator, scanner, key.slice);
            },
            .allocated_string => |buf| {
                var key = TokenString{ .slice = buf, .owned = buf };
                defer key.release(allocator);
                try handler(context, allocator, scanner, key.slice);
            },
            else => return ParseError.UnexpectedToken,
        }
    }
}

fn parseU64Value(scanner: *std.json.Scanner, allocator: std.mem.Allocator) ParserError!u64 {
    const peek = try scanner.peekNextTokenType();
    switch (peek) {
        .null => {
            _ = try scanner.next();
            return 0;
        },
        .number => {
            var number = try readNumberToken(scanner, allocator);
            defer number.release(allocator);
            return Model.parseTokenNumber(number.slice);
        },
        else => return ParseError.UnexpectedToken,
    }
}

fn loadPricing(
    arena: std.mem.Allocator,
    allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !void {
    var total_timer = try std.time.Timer.start();

    const before_fetch = pricing.count();
    var fetch_timer = try std.time.Timer.start();
    var fetch_ok = true;
    var fetch_error: ?anyerror = null;
    fetchRemotePricing(arena, allocator, pricing) catch |err| {
        fetch_ok = false;
        fetch_error = err;
    };
    const fetch_elapsed = nsToMs(fetch_timer.read());
    if (fetch_ok) {
        std.log.info(
            "codex.loadPricing: remote pricing fetched in {d:.2}ms (models += {d})",
            .{ fetch_elapsed, pricing.count() - before_fetch },
        );
    } else {
        std.log.warn(
            "codex.loadPricing: remote pricing failed after {d:.2}ms ({s})",
            .{ fetch_elapsed, @errorName(fetch_error.?) },
        );
    }

    const had_pricing = pricing.count() != 0;
    var fallback_timer = try std.time.Timer.start();
    try ensureFallbackPricing(arena, pricing);
    std.log.info(
        "codex.loadPricing: {s} fallback pricing in {d:.2}ms (models={d})",
        .{ if (had_pricing) "ensured" else "inserted", nsToMs(fallback_timer.read()), pricing.count() },
    );

    std.log.info(
        "codex.loadPricing completed in {d:.2}ms (models={d})",
        .{ nsToMs(total_timer.read()), pricing.count() },
    );
}

fn fetchRemotePricing(
    arena: std.mem.Allocator,
    allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !void {
    var io_threaded = std.Io.Threaded.init(allocator);
    defer io_threaded.deinit();

    var client = std.http.Client{
        .allocator = allocator,
        .io = io_threaded.io(),
    };
    defer client.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer_ctx = CollectWriter.init(&buffer, allocator);

    const result = try client.fetch(.{
        .location = .{ .url = Model.PRICING_URL },
        .response_writer = &writer_ctx.base,
    });

    if (result.status.class() != .success) return error.HttpError;

    const payload = try arena.dupe(u8, buffer.items);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{
        .ignore_unknown_fields = true,
        .duplicate_field_behavior = .use_last,
    });
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidResponse;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        try addPricingFromJson(arena, pricing, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn addPricingFromJson(
    arena: std.mem.Allocator,
    pricing: *Model.PricingMap,
    model_name: []const u8,
    value: std.json.Value,
) !void {
    if (pricing.get(model_name) != null) return;
    if (value != .object) return;

    const obj = value.object;
    const input = getNumericField(obj, "input_cost_per_token") orelse return;
    const output = getNumericField(obj, "output_cost_per_token") orelse return;
    const cached = getNumericField(obj, "cache_read_input_token_cost") orelse input;

    const key = try arena.dupe(u8, model_name);
    try pricing.put(key, .{
        .input_cost_per_m = input * Model.MILLION,
        .cached_input_cost_per_m = cached * Model.MILLION,
        .output_cost_per_m = output * Model.MILLION,
    });
}

fn ensureFallbackPricing(arena: std.mem.Allocator, pricing: *Model.PricingMap) !void {
    for (FALLBACK_PRICING) |fallback| {
        if (pricing.get(fallback.name) != null) continue;
        const key = try arena.dupe(u8, fallback.name);
        try pricing.put(key, fallback.pricing);
    }
}

fn getNumericField(
    object: std.json.ObjectMap,
    field: []const u8,
) ?f64 {
    const entry = object.get(field) orelse return null;
    return valueAsFloat(entry);
}

fn valueAsFloat(value: std.json.Value) ?f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        .number_string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}

fn duplicateNonEmpty(arena: std.mem.Allocator, value: []const u8) !?[]const u8 {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return try arena.dupe(u8, trimmed);
}

const CollectWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,

    fn init(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator) CollectWriter {
        return .{
            .base = .{
                .vtable = &.{
                    .drain = CollectWriter.drain,
                    .sendFile = std.Io.Writer.unimplementedSendFile,
                    .flush = CollectWriter.flush,
                    .rebase = std.Io.Writer.defaultRebase,
                },
                .buffer = &.{},
            },
            .list = list,
            .allocator = allocator,
        };
    }

    fn drain(
        writer: *std.Io.Writer,
        data: []const []const u8,
        splat: usize,
    ) std.Io.Writer.Error!usize {
        const self: *CollectWriter = @fieldParentPtr("base", writer);
        var written: usize = 0;
        for (data) |chunk| {
            if (chunk.len == 0) continue;
            self.list.appendSlice(self.allocator, chunk) catch return error.WriteFailed;
            written += chunk.len;
        }
        if (splat > 1 and data.len != 0) {
            const last = data[data.len - 1];
            const extra = splat - 1;
            for (0..extra) |_| {
                self.list.appendSlice(self.allocator, last) catch return error.WriteFailed;
                written += last.len;
            }
        }
        return written;
    }

    fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
        return;
    }
};

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}
