const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const RawUsage = model.RawTokenUsage;
const TokenString = provider.JsonTokenSlice;
const ModelState = provider.ModelState;

const PayloadResult = struct {
    payload_type: ?TokenString = null,
    model: ?TokenString = null,
    last_usage: ?RawUsage = null,
    total_usage: ?RawUsage = null,

    fn deinit(self: *PayloadResult, allocator: std.mem.Allocator) void {
        if (self.payload_type) |*tok| tok.deinit(allocator);
        if (self.model) |*tok| tok.deinit(allocator);
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
    .parse_session_fn = parseCodexSessionFile,
});

pub const collect = ProviderExports.collect;
pub const streamEvents = ProviderExports.streamEvents;
pub const loadPricingData = ProviderExports.loadPricingData;
pub const EventConsumer = ProviderExports.EventConsumer;

fn parseCodexSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*provider.MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    _ = deduper;
    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    var scanner = std.json.Scanner.initStreaming(allocator);
    defer scanner.deinit();

    var handler = CodexLineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .scanner = &scanner,
        .file_path = file_path,
        .session_id = session_id,
        .events = events,
        .previous_totals = &previous_totals,
        .model_state = &model_state,
        .timezone_offset_minutes = timezone_offset_minutes,
    };

    try provider.streamJsonLines(
        allocator,
        ctx,
        file_path,
        .{
            .max_bytes = 128 * 1024 * 1024,
            .open_error_message = "unable to open session file",
            .read_error_message = "error while reading session stream",
            .advance_error_message = "error while advancing session stream",
        },
        &handler,
        CodexLineHandler.handle,
    );
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
        defer value.deinit(allocator);
        const type_slice = value.view();
        if (std.mem.eql(u8, type_slice, "turn_context")) {
            is_turn_context.* = true;
            is_event_msg.* = false;
        } else if (std.mem.eql(u8, type_slice, "event_msg")) {
            is_event_msg.* = true;
            is_turn_context.* = false;
        }
        return;
    }

    if (std.mem.eql(u8, key, "timestamp")) {
        if (timestamp_token.*) |*existing| existing.deinit(allocator);
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
        .string => |slice| TokenString{ .borrowed = slice },
        .allocated_string => |buf| TokenString{ .owned = buf },
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
        .number => |slice| TokenString{ .borrowed = slice },
        .allocated_number => |buf| TokenString{ .owned = buf },
        else => ParseError.UnexpectedToken,
    };
}

fn replaceToken(dest: *?TokenString, allocator: std.mem.Allocator, token: TokenString) void {
    if (dest.*) |*existing| existing.deinit(allocator);
    dest.* = token;
}

fn captureModelToken(dest: *?TokenString, allocator: std.mem.Allocator, token: TokenString) void {
    if (token.view().len == 0) {
        var tmp = token;
        tmp.deinit(allocator);
        return;
    }
    if (dest.* == null) {
        dest.* = token;
    } else {
        var tmp = token;
        tmp.deinit(allocator);
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

    var accumulator = model.UsageAccumulator{};

    while (true) {
        const key_token = try scanner.nextAlloc(allocator, .alloc_if_needed);
        switch (key_token) {
            .object_end => break,
            .string => |slice| {
                var key = TokenString{ .borrowed = slice };
                defer key.deinit(allocator);
                try parseUsageField(allocator, scanner, key.view(), &accumulator);
            },
            .allocated_string => |buf| {
                var key = TokenString{ .owned = buf };
                defer key.deinit(allocator);
                try parseUsageField(allocator, scanner, key.view(), &accumulator);
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
    accumulator: *model.UsageAccumulator,
) ParserError!void {
    const field = model.usageFieldForKey(key) orelse {
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
                        var key = TokenString{ .borrowed = slice };
                        defer key.deinit(allocator);
                        if (isModelKey(key.view())) {
                            const maybe_token = try readOptionalStringToken(scanner, allocator);
                            if (maybe_token) |token| captureModelToken(storage, allocator, token);
                        } else {
                            try parseModelValue(allocator, scanner, storage);
                        }
                    },
                    .allocated_string => |buf| {
                        var key = TokenString{ .owned = buf };
                        defer key.deinit(allocator);
                        if (isModelKey(key.view())) {
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
        .string => try scanner.skipValue(),
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

fn resetScanner(scanner: *std.json.Scanner, input: []const u8) void {
    scanner.state = .value;
    scanner.string_is_object_key = false;
    scanner.stack.bytes.clearRetainingCapacity();
    scanner.stack.bit_len = 0;
    scanner.value_start = 0;
    scanner.input = input;
    scanner.cursor = 0;
    scanner.is_end_of_input = true;
    scanner.diagnostics = null;
}

const CodexLineHandler = struct {
    ctx: *const provider.ParseContext,
    allocator: std.mem.Allocator,
    scanner: *std.json.Scanner,
    file_path: []const u8,
    session_id: []const u8,
    events: *std.ArrayList(model.TokenUsageEvent),
    previous_totals: *?RawUsage,
    model_state: *ModelState,
    timezone_offset_minutes: i32,

    fn handle(self: *CodexLineHandler, line: []const u8, line_index: usize) !void {
        try self.processSessionLine(line, line_index);
    }

    fn processSessionLine(self: *CodexLineHandler, line: []const u8, line_index: usize) !void {
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0) return;

        resetScanner(self.scanner, trimmed);

        const start_token = self.scanner.next() catch |err| {
            std.log.warn(
                "{s}: failed to parse codex session file '{s}' line {d} ({s})",
                .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
            );
            return;
        };
        if (start_token != .object_begin) return;

        var payload_result = PayloadResult{};
        defer payload_result.deinit(self.allocator);
        var timestamp_token: ?TokenString = null;
        defer if (timestamp_token) |*tok| tok.deinit(self.allocator);
        var is_turn_context = false;
        var is_event_msg = false;

        while (true) {
            const key_token = self.scanner.nextAlloc(self.allocator, .alloc_if_needed) catch |err| {
                std.log.warn(
                    "{s}: failed to parse codex session file '{s}' line {d} ({s})",
                    .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
                );
                return;
            };

            switch (key_token) {
                .object_end => break,
                .string => |slice| {
                    var key = TokenString{ .borrowed = slice };
                    defer key.deinit(self.allocator);
                    parseObjectField(
                        self.allocator,
                        self.scanner,
                        key.view(),
                        &payload_result,
                        &timestamp_token,
                        &is_turn_context,
                        &is_event_msg,
                    ) catch |err| {
                        std.log.warn(
                            "{s}: failed to parse codex session file '{s}' line {d} ({s})",
                            .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
                        );
                        return;
                    };
                },
                .allocated_string => |buf| {
                    var key = TokenString{ .owned = buf };
                    defer key.deinit(self.allocator);
                    parseObjectField(
                        self.allocator,
                        self.scanner,
                        key.view(),
                        &payload_result,
                        &timestamp_token,
                        &is_turn_context,
                        &is_event_msg,
                    ) catch |err| {
                        std.log.warn(
                            "{s}: failed to parse codex session file '{s}' line {d} ({s})",
                            .{ self.ctx.provider_name, self.file_path, line_index, @errorName(err) },
                        );
                        return;
                    };
                },
                else => return,
            }
        }

        _ = self.scanner.next() catch {};

        if (timestamp_token == null) return;

        if (is_turn_context) {
            if (payload_result.model) |token| {
                var model_token = token;
                payload_result.model = null;
                _ = self.ctx.captureModel(self.allocator, self.model_state, model_token) catch |err| {
                    self.ctx.logWarning(self.file_path, "failed to capture model", err);
                    return;
                };
                model_token.deinit(self.allocator);
            }
            return;
        }

        if (!is_event_msg) return;

        var payload_type_is_token_count = false;
        if (payload_result.payload_type) |token| {
            payload_type_is_token_count = std.mem.eql(u8, token.view(), "token_count");
        }
        if (!payload_type_is_token_count) return;

        var raw_timestamp = timestamp_token.?;
        timestamp_token = null;
        const timestamp_info = try provider.timestampFromSlice(self.allocator, raw_timestamp.view(), self.timezone_offset_minutes) orelse {
            raw_timestamp.deinit(self.allocator);
            return;
        };
        raw_timestamp.deinit(self.allocator);
        const timestamp_copy = timestamp_info.text;
        const iso_date = timestamp_info.local_iso_date;

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
        if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
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
            .timestamp = timestamp_copy,
            .local_iso_date = iso_date,
            .model = resolved.name,
            .usage = delta,
            .is_fallback = resolved.is_fallback,
            .display_input_tokens = self.ctx.computeDisplayInput(delta),
        };
        try self.events.append(self.allocator, event);
    }
};

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
                var key = TokenString{ .borrowed = slice };
                defer key.deinit(allocator);
                try handler(context, allocator, scanner, key.view());
            },
            .allocated_string => |buf| {
                var key = TokenString{ .owned = buf };
                defer key.deinit(allocator);
                try handler(context, allocator, scanner, key.view());
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
            defer number.deinit(allocator);
            return model.parseTokenNumber(number.view());
        },
        else => return ParseError.UnexpectedToken,
    }
}

test "codex parser emits usage events from token_count entries" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events = std.ArrayList(model.TokenUsageEvent){};
    defer events.deinit(worker_allocator);

    const ctx = provider.ParseContext{
        .provider_name = "codex-test",
        .legacy_fallback_model = "gpt-5",
        .cached_counts_overlap_input = true,
    };

    try parseCodexSessionFile(
        worker_allocator,
        &ctx,
        "codex-fixture",
        "fixtures/codex/basic.jsonl",
        null,
        0,
        &events,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("codex-fixture", event.session_id);
    try std.testing.expectEqualStrings("gpt-5-codex", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 1000), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 200), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 50), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 1200), event.display_input_tokens);
}
