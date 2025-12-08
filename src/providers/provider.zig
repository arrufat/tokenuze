const std = @import("std");
const model = @import("../model.zig");
const timeutil = @import("../time.zig");
const io_util = @import("../io_util.zig");
const http_client = @import("../http_client.zig");

pub const EventConsumer = struct {
    context: *anyopaque,
    mutex: ?*std.Thread.Mutex = null,
    ingest: *const fn (*anyopaque, std.mem.Allocator, *const model.TokenUsageEvent, model.DateFilters) model.IngestError!void,
};

pub const FallbackPricingEntry = struct {
    name: []const u8,
    pricing: model.ModelPricing,
};

pub const ParseContext = struct {
    provider_name: []const u8,
    legacy_fallback_model: ?[]const u8,
    cached_counts_overlap_input: bool,

    pub fn logWarning(self: ParseContext, file_path: []const u8, message: []const u8, err: anyerror) void {
        std.log.warn(
            "{s}: {s} '{s}' ({s})",
            .{ self.provider_name, message, file_path, @errorName(err) },
        );
    }

    pub fn normalizeUsageDelta(self: ParseContext, delta: *model.TokenUsage) void {
        if (!self.cached_counts_overlap_input) return;
        const overlap = if (delta.cached_input_tokens > delta.input_tokens)
            delta.input_tokens
        else
            delta.cached_input_tokens;
        delta.input_tokens -= overlap;
        delta.cached_input_tokens = overlap;
    }

    pub fn computeDisplayInput(self: ParseContext, usage: model.TokenUsage) u64 {
        if (!self.cached_counts_overlap_input) return usage.input_tokens;
        return std.math.add(u64, usage.input_tokens, usage.cached_input_tokens) catch std.math.maxInt(u64);
    }

    pub fn captureModel(
        self: ParseContext,
        allocator: std.mem.Allocator,
        state: *ModelState,
        raw_model: anytype,
    ) !bool {
        _ = self;
        const slice = modelSliceFrom(@TypeOf(raw_model), raw_model) orelse return false;
        const duplicate = try duplicateNonEmpty(allocator, slice) orelse return false;
        state.current = duplicate;
        state.is_fallback = false;
        return true;
    }

    pub fn requireModel(
        self: ParseContext,
        allocator: std.mem.Allocator,
        state: *ModelState,
        raw_model: anytype,
    ) !?ResolvedModel {
        _ = try self.captureModel(allocator, state, raw_model);
        return resolveModel(&self, state);
    }
};

fn modelSliceFrom(comptime T: type, raw_model: T) ?[]const u8 {
    if (T == @TypeOf(null)) return null;
    switch (@typeInfo(T)) {
        .optional => {
            if (raw_model) |value| {
                return modelSliceFrom(@TypeOf(value), value);
            }
            return null;
        },
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return raw_model;
            }
            return null;
        },
        .array => |array_info| {
            if (array_info.child == u8) {
                return raw_model[0..];
            }
            return null;
        },
        .@"struct" => {
            if (T == model.TokenBuffer) {
                return raw_model.slice;
            }
            return null;
        },
        .@"union" => {
            if (T == std.json.Value) {
                return switch (raw_model) {
                    .string => |slice| slice,
                    .number_string => |slice| slice,
                    else => null,
                };
            } else if (T == JsonTokenSlice) {
                return raw_model.view();
            }
            return null;
        },
        else => return null,
    }
}

pub const MessageDeduper = struct {
    mutex: std.Thread.Mutex = .{},
    map: std.AutoHashMap(u64, void),

    pub fn init(allocator: std.mem.Allocator) !MessageDeduper {
        return .{ .map = std.AutoHashMap(u64, void).init(allocator) };
    }

    pub fn deinit(self: *MessageDeduper) void {
        self.map.deinit();
    }

    pub fn mark(self: *MessageDeduper, key: u64) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        const gop = try self.map.getOrPut(key);
        if (gop.found_existing) return false;
        return true;
    }
};

pub fn duplicateNonEmpty(arena: std.mem.Allocator, value: []const u8) !?[]const u8 {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return try arena.dupe(u8, trimmed);
}

pub const ModelState = struct {
    current: ?[]const u8 = null,
    is_fallback: bool = false,
};

pub const ResolvedModel = struct {
    name: []const u8,
    is_fallback: bool,
};

pub fn resolveModel(
    ctx: *const ParseContext,
    state: *ModelState,
) ?ResolvedModel {
    if (state.current) |mod| {
        return .{ .name = mod, .is_fallback = state.is_fallback };
    }

    if (ctx.legacy_fallback_model) |legacy| {
        state.current = legacy;
        state.is_fallback = true;
        return .{ .name = legacy, .is_fallback = true };
    }

    return null;
}

pub const TimestampInfo = struct {
    text: []const u8,
    local_iso_date: [10]u8,
};

pub fn timestampFromSlice(
    allocator: std.mem.Allocator,
    slice: []const u8,
    timezone_offset_minutes: i32,
) !?TimestampInfo {
    const duplicate = try duplicateNonEmpty(allocator, slice) orelse return null;
    const iso_date = timeutil.isoDateForTimezone(duplicate, timezone_offset_minutes) catch {
        allocator.free(duplicate);
        return null;
    };
    return .{ .text = duplicate, .local_iso_date = iso_date };
}

pub fn updateTimestampFromReader(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    timezone_offset_minutes: i32,
    slot: *?TimestampInfo,
) !void {
    var token = try jsonReadStringToken(allocator, reader);
    defer token.deinit(allocator);
    const info = try timestampFromSlice(allocator, token.view(), timezone_offset_minutes) orelse return;
    if (slot.*) |existing| allocator.free(existing.text);
    slot.* = info;
}

pub fn emitUsageEventWithTimestamp(
    ctx: *const ParseContext,
    allocator: std.mem.Allocator,
    state: *ModelState,
    sink: EventSink,
    session_label: []const u8,
    timestamp_slot: *?TimestampInfo,
    usage: model.TokenUsage,
    raw_model: anytype,
) !void {
    if (!shouldEmitUsage(usage)) return;
    const timestamp_info = timestamp_slot.* orelse return;
    const resolved = (try ctx.requireModel(allocator, state, raw_model)) orelse return;
    timestamp_slot.* = null;

    const event = model.TokenUsageEvent{
        .session_id = session_label,
        .timestamp = timestamp_info.text,
        .local_iso_date = timestamp_info.local_iso_date,
        .model = resolved.name,
        .usage = usage,
        .is_fallback = resolved.is_fallback,
        .display_input_tokens = ctx.computeDisplayInput(usage),
    };
    try sink.emit(event);
}

pub fn overrideSessionLabelFromSlice(
    allocator: std.mem.Allocator,
    session_label: *[]const u8,
    overridden: ?*bool,
    slice: []const u8,
) void {
    if (overridden) |flag| if (flag.*) return;
    const duplicate = duplicateNonEmpty(allocator, slice) catch null;
    if (duplicate) |dup| {
        session_label.* = dup;
        if (overridden) |flag| flag.* = true;
    }
}

pub fn overrideSessionLabelFromReader(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    session_label: *[]const u8,
    overridden: ?*bool,
) !void {
    var token = try jsonReadStringToken(allocator, reader);
    defer token.deinit(allocator);
    overrideSessionLabelFromSlice(allocator, session_label, overridden, token.view());
}

pub const UsageValueMode = enum {
    set,
    add,
};

pub const UsageFieldDescriptor = struct {
    key: []const u8,
    field: model.UsageField,
    mode: UsageValueMode = .set,
};

const UsageDescriptorContext = struct {
    accumulator: *model.UsageAccumulator,
    descriptors: []const UsageFieldDescriptor,
};

pub const JsonlStreamMode = enum {
    lines,
    document,
};

pub const JsonlStreamOptions = struct {
    max_bytes: usize = 128 * 1024 * 1024,
    delimiter: u8 = '\n',
    trim_lines: bool = true,
    skip_empty: bool = true,
    mode: JsonlStreamMode = .lines,
    open_error_message: []const u8 = "unable to open session file",
    read_error_message: []const u8 = "error while reading session stream",
    advance_error_message: []const u8 = "error while advancing session stream",
};

pub fn streamJsonLines(
    allocator: std.mem.Allocator,
    ctx: *const ParseContext,
    file_path: []const u8,
    options: JsonlStreamOptions,
    handler_context: anytype,
    comptime handler: fn (@TypeOf(handler_context), []const u8, usize) anyerror!void,
) !void {
    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        ctx.logWarning(file_path, options.open_error_message, err);
        return;
    };
    defer file.close();

    if (options.mode == .document) {
        try streamJsonDocumentInternal(
            allocator,
            ctx,
            file_path,
            options,
            file,
            handler_context,
            handler,
        );
        return;
    }

    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const io = io_single.io();

    var reader_buffer: [64 * 1024]u8 = undefined;
    var file_reader = file.readerStreaming(io, reader_buffer[0..]);
    var reader = &file_reader.interface;

    var partial_line: std.ArrayList(u8) = .empty;
    defer partial_line.deinit(allocator);

    var streamed_total: usize = 0;
    var line_index: usize = 0;

    var limit_warned = false;

    const checkLimit = struct {
        fn hit(parse_ctx: *const ParseContext, path: []const u8, max_bytes: usize, total: usize, warned: *bool) bool {
            if (total <= max_bytes) return false;
            if (!warned.*) {
                parse_ctx.logWarning(path, "session stream exceeded max_bytes; stopping early", error.ResponseLimitExceeded);
                warned.* = true;
            }
            return true;
        }
    }.hit;

    while (true) {
        partial_line.clearRetainingCapacity();
        var writer_ctx = io_util.ArrayWriter.init(&partial_line, allocator);
        const streamed = reader.streamDelimiterEnding(writer_ctx.writer(), options.delimiter) catch |err| {
            ctx.logWarning(file_path, options.read_error_message, err);
            return;
        };

        var newline_consumed = true;
        const discard_result = reader.discardDelimiterInclusive(options.delimiter) catch |err| switch (err) {
            error.EndOfStream => blk: {
                newline_consumed = false;
                break :blk 0;
            },
            else => {
                ctx.logWarning(file_path, options.advance_error_message, err);
                return;
            },
        };

        if (streamed == 0 and partial_line.items.len == 0 and !newline_consumed) {
            break;
        }

        streamed_total += streamed;
        if (checkLimit(ctx, file_path, options.max_bytes, streamed_total, &limit_warned)) return; // truncate but do not fail

        var line_slice: []const u8 = partial_line.items;
        if (options.trim_lines) {
            line_slice = std.mem.trim(u8, line_slice, " \t\r\n");
        }
        if (options.skip_empty and line_slice.len == 0) {
            if (!newline_consumed) break;
            streamed_total += discard_result;
            if (checkLimit(ctx, file_path, options.max_bytes, streamed_total, &limit_warned)) return;
            continue;
        }

        line_index += 1;
        try handler(handler_context, line_slice, line_index);

        if (!newline_consumed) break;
        streamed_total += discard_result;
        if (checkLimit(ctx, file_path, options.max_bytes, streamed_total, &limit_warned)) return; // truncate but do not fail
    }
}

fn streamJsonDocumentInternal(
    allocator: std.mem.Allocator,
    ctx: *const ParseContext,
    file_path: []const u8,
    options: JsonlStreamOptions,
    file: std.fs.File,
    handler_context: anytype,
    comptime handler: fn (@TypeOf(handler_context), []const u8, usize) anyerror!void,
) !void {
    var document: std.ArrayList(u8) = .empty;
    defer document.deinit(allocator);

    var buffer: [64 * 1024]u8 = undefined;
    var streamed_total: usize = 0;

    while (true) {
        const read_len = file.read(buffer[0..]) catch |err| {
            ctx.logWarning(file_path, options.read_error_message, err);
            return;
        };
        if (read_len == 0) break;
        streamed_total += read_len;
        if (streamed_total > options.max_bytes) return;
        try document.appendSlice(allocator, buffer[0..read_len]);
    }

    var line_slice: []const u8 = document.items;
    if (options.trim_lines) {
        line_slice = std.mem.trim(u8, line_slice, " \t\r\n");
    }
    if (options.skip_empty and line_slice.len == 0) {
        return;
    }

    try handler(handler_context, line_slice, 1);
}

pub const EventSink = struct {
    context: *anyopaque,
    emitFn: *const fn (*anyopaque, model.TokenUsageEvent) anyerror!void,

    pub fn emit(self: EventSink, event: model.TokenUsageEvent) !void {
        try self.emitFn(self.context, event);
    }
};

pub const EventListCollector = struct {
    allocator: std.mem.Allocator,
    list: *std.ArrayList(model.TokenUsageEvent),

    pub fn init(list: *std.ArrayList(model.TokenUsageEvent), allocator: std.mem.Allocator) EventListCollector {
        return .{ .allocator = allocator, .list = list };
    }

    pub fn asSink(self: *EventListCollector) EventSink {
        return .{ .context = self, .emitFn = EventListCollector.emit };
    }

    fn emit(ctx_ptr: *anyopaque, event: model.TokenUsageEvent) anyerror!void {
        const self: *EventListCollector = @ptrCast(@alignCast(ctx_ptr));
        try self.list.append(self.allocator, event);
    }
};

pub const ParseSessionFn = *const fn (
    allocator: std.mem.Allocator,
    ctx: *const ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    sink: EventSink,
) anyerror!void;

pub const ProviderConfig = struct {
    name: []const u8,
    sessions_dir_suffix: []const u8,
    legacy_fallback_model: ?[]const u8 = null,
    fallback_pricing: []const FallbackPricingEntry = &.{},
    session_file_ext: []const u8 = ".jsonl",
    parse_session_fn: ParseSessionFn,
    cached_counts_overlap_input: bool = false,
    requires_deduper: bool = false,
};

pub fn makeProvider(comptime cfg: ProviderConfig) type {
    const ProviderType = Provider(cfg);
    return struct {
        pub const collect = ProviderType.collect;
        pub const streamEvents = ProviderType.streamEvents;
        pub const loadPricingData = ProviderType.loadPricingData;
        pub const sessionsPath = ProviderType.sessionsPath;
    };
}

var remote_pricing_loaded = std.atomic.Value(bool).init(false);

pub const RemotePricingStats = struct {
    attempted: bool = false,
    models_added: usize = 0,
    elapsed_ms: f64 = 0,
    failure: ?anyerror = null,
    satisfied: bool = false,
};

pub fn loadRemotePricing(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    pricing: *model.PricingMap,
) !RemotePricingStats {
    var stats: RemotePricingStats = .{};
    if (remote_pricing_loaded.load(.acquire)) {
        stats.satisfied = true;
        return stats;
    }

    stats.attempted = true;
    var fetch_timer = try std.time.Timer.start();
    const before_fetch = pricing.count();
    var fetch_error: ?anyerror = null;
    fetchRemotePricing(shared_allocator, temp_allocator, pricing) catch |err| {
        fetch_error = err;
    };
    stats.elapsed_ms = nsToMs(fetch_timer.read());
    if (fetch_error) |err| {
        stats.failure = err;
    } else {
        stats.models_added = pricing.count() - before_fetch;
        stats.satisfied = true;
        remote_pricing_loaded.store(true, .release);
    }
    return stats;
}

const nsToMs = timeutil.nsToMs;

fn fetchRemotePricing(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    pricing: *model.PricingMap,
) !void {
    var response = try http_client.request(
        temp_allocator,
        temp_allocator,
        .{
            .method = .GET,
            .url = model.pricing_url,
            .force_identity_encoding = true,
            .response_limit = std.Io.Limit.limited(4 * 1024 * 1024),
        },
    );
    defer response.deinit();

    var slice_reader = std.Io.Reader.fixed(response.body);
    var json_reader = std.json.Reader.init(temp_allocator, &slice_reader);
    defer json_reader.deinit();

    var parser = PricingFeedParser{
        .allocator = shared_allocator,
        .temp_allocator = temp_allocator,
        .pricing = pricing,
    };
    try parser.parse(&json_reader);
}

const PricingFeedParser = struct {
    allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    pricing: *model.PricingMap,
    const AliasError = error{OutOfMemory};

    pub fn parse(self: *PricingFeedParser, reader: *std.json.Reader) !void {
        if ((try reader.next()) != .object_begin) return error.InvalidResponse;

        while (true) {
            switch (try reader.peekNextTokenType()) {
                .object_end => {
                    _ = try reader.next();
                    switch (try reader.next()) {
                        .end_of_document => return,
                        else => return error.InvalidResponse,
                    }
                },
                .string => {
                    var name = try JsonTokenSlice.fromString(self.temp_allocator, reader);
                    defer name.deinit(self.temp_allocator);
                    const maybe_pricing = try self.parseEntry(self.temp_allocator, reader);
                    if (maybe_pricing) |entry| {
                        try self.insertPricingEntries(name.view(), entry);
                    }
                },
                else => return error.InvalidResponse,
            }
        }
    }

    fn parseEntry(
        self: *PricingFeedParser,
        scratch_allocator: std.mem.Allocator,
        reader: *std.json.Reader,
    ) !?model.ModelPricing {
        _ = self;
        if ((try reader.next()) != .object_begin) {
            try reader.skipValue();
            return null;
        }

        var input_rate: ?f64 = null;
        var cache_creation_rate: ?f64 = null;
        var cached_rate: ?f64 = null;
        var output_rate: ?f64 = null;

        while (true) {
            switch (try reader.peekNextTokenType()) {
                .object_end => {
                    _ = try reader.next();
                    break;
                },
                .string => {
                    var field = try JsonTokenSlice.fromString(scratch_allocator, reader);
                    defer field.deinit(scratch_allocator);
                    if (std.mem.eql(u8, field.view(), "input_cost_per_token")) {
                        input_rate = try readNumberValue(scratch_allocator, reader);
                    } else if (std.mem.eql(u8, field.view(), "output_cost_per_token")) {
                        output_rate = try readNumberValue(scratch_allocator, reader);
                    } else if (std.mem.eql(u8, field.view(), "cache_creation_input_token_cost")) {
                        cache_creation_rate = try readNumberValue(scratch_allocator, reader);
                    } else if (std.mem.eql(u8, field.view(), "cache_read_input_token_cost")) {
                        cached_rate = try readNumberValue(scratch_allocator, reader);
                    } else {
                        try reader.skipValue();
                    }
                },
                else => return error.InvalidResponse,
            }
        }

        if (input_rate == null or output_rate == null) return null;
        const creation = cache_creation_rate orelse input_rate.?;
        const cached = cached_rate orelse input_rate.?;
        return model.ModelPricing{
            .input_cost_per_m = input_rate.? * model.million,
            .cache_creation_cost_per_m = creation * model.million,
            .cached_input_cost_per_m = cached * model.million,
            .output_cost_per_m = output_rate.? * model.million,
        };
    }

    fn insertPricingEntries(
        self: *PricingFeedParser,
        name: []const u8,
        entry: model.ModelPricing,
    ) AliasError!void {
        _ = try self.putPricing(name, entry);
    }

    fn putPricing(self: *PricingFeedParser, key: []const u8, entry: model.ModelPricing) AliasError!bool {
        if (self.pricing.get(key) != null) return false;
        const copied_name = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(copied_name);
        try self.pricing.put(copied_name, entry);
        return true;
    }
};

/// Token returned by `std.json.Reader.nextAlloc`, remembering whether storage
/// was borrowed from the reader buffer or newly allocated.
pub const JsonTokenSlice = union(enum) {
    borrowed: []const u8,
    owned: []u8,

    pub fn view(self: JsonTokenSlice) []const u8 {
        return switch (self) {
            .borrowed, .owned => |val| val,
        };
    }

    pub fn deinit(self: *JsonTokenSlice, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .borrowed => {},
            .owned => |buf| allocator.free(buf),
        }
        self.* = JsonTokenSlice{ .borrowed = "" };
    }

    pub fn fromString(allocator: std.mem.Allocator, reader: *std.json.Reader) !JsonTokenSlice {
        const token = try reader.nextAlloc(allocator, .alloc_if_needed);
        return switch (token) {
            .string => |slice| .{ .borrowed = slice },
            .allocated_string => |buf| .{ .owned = buf },
            else => error.UnexpectedToken,
        };
    }

    pub fn fromNumber(allocator: std.mem.Allocator, reader: *std.json.Reader) !JsonTokenSlice {
        const token = try reader.nextAlloc(allocator, .alloc_if_needed);
        return switch (token) {
            .number => |slice| .{ .borrowed = slice },
            .allocated_number => |buf| .{ .owned = buf },
            else => error.UnexpectedToken,
        };
    }
};

pub fn jsonReadStringToken(allocator: std.mem.Allocator, reader: *std.json.Reader) !JsonTokenSlice {
    return JsonTokenSlice.fromString(allocator, reader);
}

pub fn jsonReadOptionalStringToken(allocator: std.mem.Allocator, reader: *std.json.Reader) !?JsonTokenSlice {
    const peek = try reader.peekNextTokenType();
    switch (peek) {
        .null => {
            _ = try reader.next();
            return null;
        },
        .string => return try jsonReadStringToken(allocator, reader),
        else => return error.UnexpectedToken,
    }
}

pub fn jsonReadNumberToken(allocator: std.mem.Allocator, reader: *std.json.Reader) !JsonTokenSlice {
    return JsonTokenSlice.fromNumber(allocator, reader);
}

pub fn replaceJsonToken(
    dest: *?JsonTokenSlice,
    allocator: std.mem.Allocator,
    token: JsonTokenSlice,
) !void {
    var to_store = token;
    switch (to_store) {
        .borrowed => |slice| {
            const dup = try allocator.dupe(u8, slice);
            to_store = .{ .owned = dup };
        },
        .owned => {},
    }
    if (dest.*) |*existing| existing.deinit(allocator);
    dest.* = to_store;
}

pub fn captureModelToken(
    dest: *?JsonTokenSlice,
    allocator: std.mem.Allocator,
    token: JsonTokenSlice,
) void {
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

pub fn captureModelValue(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    storage: *?JsonTokenSlice,
) !void {
    const peek = try reader.peekNextTokenType();
    switch (peek) {
        .object_begin => {
            _ = try reader.next();
            try jsonWalkObject(allocator, reader, storage, handleModelField);
        },
        .array_begin => {
            _ = try reader.next();
            while (true) {
                const next_type = try reader.peekNextTokenType();
                if (next_type == .array_end) {
                    _ = try reader.next();
                    return;
                }
                try captureModelValue(allocator, reader, storage);
            }
        },
        .string => try reader.skipValue(),
        .null => {
            _ = try reader.next();
        },
        else => try reader.skipValue(),
    }
}

fn handleModelField(
    storage: *?JsonTokenSlice,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    if (isModelKey(key)) {
        const maybe_token = try jsonReadOptionalStringToken(allocator, reader);
        if (maybe_token) |token| captureModelToken(storage, allocator, token);
        return;
    }

    try captureModelValue(allocator, reader, storage);
}

pub fn isModelKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "model") or std.mem.eql(u8, key, "model_name");
}

pub fn jsonParseU64Value(allocator: std.mem.Allocator, reader: *std.json.Reader) !u64 {
    const peek = try reader.peekNextTokenType();
    switch (peek) {
        .null => {
            _ = try reader.next();
            return 0;
        },
        .number => {
            var number = try jsonReadNumberToken(allocator, reader);
            defer number.deinit(allocator);
            return model.parseTokenNumber(number.view());
        },
        else => return error.UnexpectedToken,
    }
}

pub fn jsonParseUsageObject(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
) !?model.RawTokenUsage {
    var accumulator = model.UsageAccumulator{};
    if (!try jsonParseUsageObjectCommon(allocator, reader, &accumulator, jsonParseUsageField)) {
        return null;
    }
    return accumulator.finalize();
}

pub fn jsonParseUsageObjectWithDescriptors(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    descriptors: []const UsageFieldDescriptor,
) !?model.RawTokenUsage {
    var accumulator = model.UsageAccumulator{};
    var context = UsageDescriptorContext{
        .accumulator = &accumulator,
        .descriptors = descriptors,
    };
    if (!try jsonParseUsageObjectCommon(allocator, reader, &context, jsonApplyDescriptorField)) {
        return null;
    }
    return accumulator.finalize();
}

fn jsonParseUsageObjectCommon(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Reader, []const u8) anyerror!void,
) !bool {
    const peek = try reader.peekNextTokenType();
    if (peek == .null) {
        _ = try reader.next();
        return false;
    }
    if (peek != .object_begin) {
        try reader.skipValue();
        return false;
    }

    _ = try reader.next();

    while (true) {
        switch (try reader.peekNextTokenType()) {
            .object_end => {
                _ = try reader.next();
                return true;
            },
            .string => {
                var key = try JsonTokenSlice.fromString(allocator, reader);
                defer key.deinit(allocator);
                try handler(context, allocator, reader, key.view());
            },
            else => return error.UnexpectedToken,
        }
    }
}

fn jsonParseUsageField(
    accumulator: *model.UsageAccumulator,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    const field = model.usageFieldForKey(key) orelse {
        try reader.skipValue();
        return;
    };
    const value = try jsonParseU64Value(allocator, reader);
    accumulator.applyField(field, value);
}

fn jsonApplyDescriptorField(
    context: *UsageDescriptorContext,
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    key: []const u8,
) !void {
    var matched = false;
    var value_parsed = false;
    var parsed_value: u64 = 0;

    for (context.descriptors) |descriptor| {
        if (!std.mem.eql(u8, descriptor.key, key)) continue;
        matched = true;
        if (!value_parsed) {
            parsed_value = try jsonParseU64Value(allocator, reader);
            value_parsed = true;
        }
        switch (descriptor.mode) {
            .set => context.accumulator.applyField(descriptor.field, parsed_value),
            .add => context.accumulator.addField(descriptor.field, parsed_value),
        }
    }

    if (!matched) {
        try reader.skipValue();
    }
}

pub fn jsonWalkObject(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Reader, []const u8) anyerror!void,
) !void {
    while (true) {
        switch (try reader.peekNextTokenType()) {
            .object_end => {
                _ = try reader.next();
                return;
            },
            .string => {
                var key = try JsonTokenSlice.fromString(allocator, reader);
                defer key.deinit(allocator);
                try handler(context, allocator, reader, key.view());
            },
            else => return error.UnexpectedToken,
        }
    }
}

pub fn jsonWalkOptionalObject(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Reader, []const u8) anyerror!void,
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
    try jsonWalkObject(allocator, reader, context, handler);
}

pub fn jsonWalkArrayObjects(
    allocator: std.mem.Allocator,
    reader: *std.json.Reader,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Reader, usize) anyerror!void,
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
    var index: usize = 0;

    while (true) {
        switch (try reader.peekNextTokenType()) {
            .array_end => {
                _ = try reader.next();
                return;
            },
            .object_begin => {
                _ = try reader.next();
                try handler(context, allocator, reader, index);
                index += 1;
            },
            else => try reader.skipValue(),
        }
    }
}

pub fn shouldEmitUsage(usage: model.TokenUsage) bool {
    return !usageIsZero(usage);
}

pub fn usageIsZero(usage: model.TokenUsage) bool {
    return usage.input_tokens == 0 and
        usage.cache_creation_input_tokens == 0 and
        usage.cached_input_tokens == 0 and
        usage.output_tokens == 0 and
        usage.reasoning_output_tokens == 0;
}

pub fn parseJsonLine(
    allocator: std.mem.Allocator,
    line: []const u8,
    context: anytype,
    comptime handler: fn (@TypeOf(context), std.mem.Allocator, *std.json.Reader) anyerror!void,
) !void {
    const trimmed = std.mem.trim(u8, line, " \t\r\n");
    if (trimmed.len == 0) return;

    var slice_reader = std.Io.Reader.fixed(trimmed);
    var json_reader = std.json.Reader.init(allocator, &slice_reader);
    defer json_reader.deinit();

    if ((try json_reader.next()) != .object_begin) return;

    try handler(context, allocator, &json_reader);

    const trailing = try json_reader.next();
    if (trailing != .end_of_document) try json_reader.skipValue();
}

fn readNumberValue(allocator: std.mem.Allocator, reader: *std.json.Reader) !f64 {
    var buffered = try JsonTokenSlice.fromNumber(allocator, reader);
    defer buffered.deinit(allocator);
    return std.fmt.parseFloat(f64, buffered.view()) catch return error.InvalidNumber;
}

pub fn Provider(comptime cfg: ProviderConfig) type {
    const provider_name = cfg.name;
    const sessions_dir_suffix = cfg.sessions_dir_suffix;
    const legacy_fallback_model = cfg.legacy_fallback_model;
    const fallback_pricing = cfg.fallback_pricing;

    return struct {
        const parse_context = ParseContext{
            .provider_name = provider_name,
            .legacy_fallback_model = legacy_fallback_model,
            .cached_counts_overlap_input = cfg.cached_counts_overlap_input,
        };
        const parse_fn = cfg.parse_session_fn;
        const json_ext = cfg.session_file_ext;
        const requires_deduper = cfg.requires_deduper;

        const SummaryConsumer = struct {
            builder: *model.SummaryBuilder,
        };

        fn summaryIngest(
            ctx_ptr: *anyopaque,
            allocator: std.mem.Allocator,
            event: *const model.TokenUsageEvent,
            filters: model.DateFilters,
        ) model.IngestError!void {
            const ctx: *SummaryConsumer = @ptrCast(@alignCast(ctx_ptr));
            try ctx.builder.ingest(allocator, event, filters);
        }

        pub fn collect(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            summaries: *model.SummaryBuilder,
            filters: model.DateFilters,
            progress: ?std.Progress.Node,
        ) !void {
            var total_timer = try std.time.Timer.start();

            if (progress) |node| std.Progress.Node.setEstimatedTotalItems(node, 1);

            var events_progress: ?std.Progress.Node = null;
            if (progress) |node| {
                events_progress = std.Progress.Node.start(node, "scan sessions", 0);
            }
            var events_timer = try std.time.Timer.start();
            const before_events = summaries.eventCount();
            var builder_mutex = std.Thread.Mutex{};
            var summary_ctx = SummaryConsumer{ .builder = summaries };
            const consumer = EventConsumer{
                .context = @ptrCast(&summary_ctx),
                .mutex = &builder_mutex,
                .ingest = summaryIngest,
            };
            try collectEvents(shared_allocator, temp_allocator, filters, consumer, events_progress);
            const after_events = summaries.eventCount();
            if (events_progress) |node| std.Progress.Node.end(node);
            std.log.debug(
                "{s}.collectEvents produced {d} new events in {d:.2}ms",
                .{ provider_name, after_events - before_events, nsToMs(events_timer.read()) },
            );

            std.log.debug(
                "{s}.collect completed in {d:.2}ms",
                .{ provider_name, nsToMs(total_timer.read()) },
            );
        }

        pub fn streamEvents(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            filters: model.DateFilters,
            consumer: EventConsumer,
        ) !void {
            try collectEvents(shared_allocator, temp_allocator, filters, consumer, null);
        }

        pub fn loadPricingData(
            shared_allocator: std.mem.Allocator,
            pricing: *model.PricingMap,
        ) !void {
            for (fallback_pricing) |fallback| {
                if (pricing.get(fallback.name) != null) continue;
                const key = try shared_allocator.dupe(u8, fallback.name);
                try pricing.put(key, fallback.pricing);
            }
        }

        pub fn sessionsPath(allocator: std.mem.Allocator) ![]u8 {
            return resolveSessionsDir(allocator);
        }

        fn logSessionWarning(file_path: []const u8, message: []const u8, err: anyerror) void {
            std.log.warn(
                "{s}: {s} '{s}' ({s})",
                .{ provider_name, message, file_path, @errorName(err) },
            );
        }

        fn collectEvents(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            filters: model.DateFilters,
            consumer: EventConsumer,
            progress: ?std.Progress.Node,
        ) !void {
            const SharedContext = struct {
                shared_allocator: std.mem.Allocator,
                temp_allocator: std.mem.Allocator,
                filters: model.DateFilters,
                progress: ?std.Progress.Node,
                sessions_dir: []const u8,
                paths: []const []const u8,
                completed: *std.atomic.Value(usize),
                deduper: ?*MessageDeduper,
                consumer: EventConsumer,
            };

            var timer = try std.time.Timer.start();

            const sessions_dir = resolveSessionsDir(shared_allocator) catch |err| {
                std.log.info("{s}.collectEvents: skipping, unable to resolve sessions dir ({s})", .{ provider_name, @errorName(err) });
                return;
            };
            defer shared_allocator.free(sessions_dir);

            var root_dir = std.fs.openDirAbsolute(sessions_dir, .{ .iterate = true }) catch |err| {
                std.log.info(
                    "{s}.collectEvents: skipping, unable to open sessions dir '{s}' ({s})",
                    .{ provider_name, sessions_dir, @errorName(err) },
                );
                return;
            };
            defer root_dir.close();

            var walker = try root_dir.walk(shared_allocator);
            defer walker.deinit();

            var relative_paths: std.ArrayList([]u8) = .empty;
            defer {
                for (relative_paths.items) |path| shared_allocator.free(path);
                relative_paths.deinit(shared_allocator);
            }

            while (try walker.next()) |entry| {
                if (entry.kind != .file) continue;
                const relative_path = std.mem.sliceTo(entry.path, 0);
                if (!std.mem.endsWith(u8, relative_path, json_ext)) continue;
                const copy = try shared_allocator.dupe(u8, relative_path);
                try relative_paths.append(shared_allocator, copy);
            }

            if (relative_paths.items.len == 0) {
                if (progress) |node| {
                    std.Progress.Node.setEstimatedTotalItems(node, 0);
                    std.Progress.Node.setCompletedItems(node, 0);
                }
                std.log.debug("{s}.collectEvents: scanned 0 files in {d:.2}ms", .{ provider_name, nsToMs(timer.read()) });
                return;
            }

            var completed = std.atomic.Value(usize).init(0);
            if (progress) |node| {
                std.Progress.Node.setEstimatedTotalItems(node, relative_paths.items.len);
                std.Progress.Node.setCompletedItems(node, 0);
            }

            var deduper_storage: ?MessageDeduper = null;
            defer if (deduper_storage) |*ded| ded.deinit();
            if (requires_deduper) {
                deduper_storage = try MessageDeduper.init(temp_allocator);
            }

            var shared = SharedContext{
                .shared_allocator = shared_allocator,
                .temp_allocator = temp_allocator,
                .filters = filters,
                .progress = progress,
                .sessions_dir = sessions_dir,
                .paths = relative_paths.items,
                .completed = &completed,
                .deduper = if (deduper_storage) |*ded| ded else null,
                .consumer = consumer,
            };

            const timezone_offset = @as(i32, filters.timezone_offset_minutes);
            const worker_count = blk: {
                const cpu_count = std.Thread.getCpuCount() catch 1;
                const clamped = @max(1, cpu_count);
                break :blk @min(relative_paths.items.len, clamped);
            };

            var work_index = std.atomic.Value(usize).init(0);

            const WorkerArgs = struct {
                shared: *SharedContext,
                work_index: *std.atomic.Value(usize),
                timezone_offset: i32,
            };

            const Worker = struct {
                fn run(args: WorkerArgs) void {
                    var worker_arena_state = std.heap.ArenaAllocator.init(args.shared.temp_allocator);
                    defer worker_arena_state.deinit();
                    const worker_allocator = worker_arena_state.allocator();

                    const sink = EventSink{
                        .context = args.shared,
                        .emitFn = struct {
                            fn emit(ctx_ptr: *anyopaque, event: model.TokenUsageEvent) anyerror!void {
                                const ctx: *SharedContext = @ptrCast(@alignCast(ctx_ptr));
                                if (ctx.consumer.mutex) |mutex| mutex.lock();
                                defer if (ctx.consumer.mutex) |mutex| mutex.unlock();
                                try ctx.consumer.ingest(
                                    ctx.consumer.context,
                                    ctx.shared_allocator,
                                    &event,
                                    ctx.filters,
                                );
                            }
                        }.emit,
                    };

                    while (true) {
                        const idx = args.work_index.fetchAdd(1, .acq_rel);
                        if (idx >= args.shared.paths.len) break;

                        _ = worker_arena_state.reset(.retain_capacity);
                        processPath(args.shared, worker_allocator, sink, args.timezone_offset, idx);

                        const finished = args.shared.completed.fetchAdd(1, .acq_rel) + 1;
                        if (args.shared.progress) |node| node.setCompletedItems(finished);
                    }
                }

                fn processPath(
                    shared_ctx: *SharedContext,
                    worker_allocator: std.mem.Allocator,
                    sink: EventSink,
                    tz_offset: i32,
                    idx: usize,
                ) void {
                    const relative = shared_ctx.paths[idx];
                    const absolute_path = std.fs.path.join(worker_allocator, &.{ shared_ctx.sessions_dir, relative }) catch |err| {
                        std.log.warn("{s}.collectEvents: unable to build path for '{s}' ({s})", .{
                            provider_name,
                            relative,
                            @errorName(err),
                        });
                        return;
                    };
                    defer worker_allocator.free(absolute_path);

                    if (relative.len <= json_ext.len or !std.mem.endsWith(u8, relative, json_ext)) return;
                    const session_id_slice = relative[0 .. relative.len - json_ext.len];

                    const maybe_session_id = duplicateNonEmpty(worker_allocator, session_id_slice) catch {
                        return;
                    };
                    const session_id = maybe_session_id orelse return;

                    parseSessionFile(
                        worker_allocator,
                        session_id,
                        absolute_path,
                        shared_ctx.deduper,
                        tz_offset,
                        sink,
                    ) catch |err| {
                        logSessionWarning(absolute_path, "failed to parse session file", err);
                        return;
                    };
                }
            };

            const worker_args = WorkerArgs{
                .shared = &shared,
                .work_index = &work_index,
                .timezone_offset = timezone_offset,
            };

            if (worker_count == 1) {
                Worker.run(worker_args);
            } else {
                const threads = try shared_allocator.alloc(std.Thread, worker_count);
                defer shared_allocator.free(threads);

                for (threads) |*thread| {
                    thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker_args});
                }
                for (threads) |thread| thread.join();
            }

            const final_completed = completed.load(.acquire);

            std.log.debug(
                "{s}.collectEvents: scanned {d} files in {d:.2}ms",
                .{ provider_name, final_completed, nsToMs(timer.read()) },
            );
        }

        fn resolveSessionsDir(allocator: std.mem.Allocator) ![]u8 {
            const home = try std.process.getEnvVarOwned(allocator, "HOME");
            defer allocator.free(home);
            return std.fmt.allocPrint(allocator, "{s}{s}", .{ home, sessions_dir_suffix });
        }

        fn parseSessionFile(
            allocator: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            deduper: ?*MessageDeduper,
            timezone_offset_minutes: i32,
            sink: EventSink,
        ) !void {
            try parse_fn(allocator, &parse_context, session_id, file_path, deduper, timezone_offset_minutes, sink);
        }

        test "pricing parser stores manifest entries" {
            const allocator = std.testing.allocator;
            var pricing = model.PricingMap.init(allocator);
            defer model.deinitPricingMap(&pricing, allocator);
            var parser = PricingFeedParser{
                .allocator = allocator,
                .temp_allocator = allocator,
                .pricing = &pricing,
            };

            const pricing_entry = model.ModelPricing{
                .input_cost_per_m = 1,
                .cache_creation_cost_per_m = 1,
                .cached_input_cost_per_m = 1,
                .output_cost_per_m = 1,
            };
            try parser.insertPricingEntries("gemini/gemini-flash-latest", pricing_entry);

            try std.testing.expect(pricing.get("gemini/gemini-flash-latest") != null);
        }

        test "usage descriptors map and accumulate token fields" {
            const test_allocator = std.testing.allocator;
            const json_src =
                \\{
                \\  "input_direct": 150,
                \\  "cache_read": 40,
                \\  "output_primary": 25,
                \\  "output_tool": 5,
                \\  "thoughts": 7
                \\}
            ;

            var slice_reader = std.Io.Reader.fixed(json_src);
            var json_reader = std.json.Reader.init(test_allocator, &slice_reader);
            defer json_reader.deinit();

            const descriptors = [_]UsageFieldDescriptor{
                .{ .key = "input_direct", .field = .input_tokens },
                .{ .key = "cache_read", .field = .cached_input_tokens },
                .{ .key = "output_primary", .field = .output_tokens },
                .{ .key = "output_tool", .field = .output_tokens, .mode = .add },
                .{ .key = "thoughts", .field = .reasoning_output_tokens },
                .{ .key = "input_direct", .field = .total_tokens, .mode = .add },
                .{ .key = "cache_read", .field = .total_tokens, .mode = .add },
                .{ .key = "output_primary", .field = .total_tokens, .mode = .add },
                .{ .key = "output_tool", .field = .total_tokens, .mode = .add },
            };

            const usage = (try jsonParseUsageObjectWithDescriptors(
                test_allocator,
                &json_reader,
                descriptors[0..],
            )) orelse unreachable;
            try std.testing.expectEqual(@as(u64, 150), usage.input_tokens);
            try std.testing.expectEqual(@as(u64, 40), usage.cached_input_tokens);
            try std.testing.expectEqual(@as(u64, 30), usage.output_tokens);
            try std.testing.expectEqual(@as(u64, 7), usage.reasoning_output_tokens);
            try std.testing.expectEqual(@as(u64, 220), usage.total_tokens);
        }

        test "timestamp helpers duplicate slices" {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const info = try timestampFromSlice(allocator, "2025-02-15T09:30:00Z", 0) orelse unreachable;
            try std.testing.expectEqualStrings("2025-02-15", info.local_iso_date[0..]);
        }

        test "session label overrides respect guard" {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            var label: []const u8 = "default";
            var overridden = false;
            overrideSessionLabelFromSlice(allocator, &label, &overridden, "override-one");
            try std.testing.expectEqualStrings("override-one", label);
            try std.testing.expect(overridden);

            overrideSessionLabelFromSlice(allocator, &label, &overridden, "override-two");
            try std.testing.expectEqualStrings("override-one", label);
        }

        test "jsonWalkArrayObjects iterates array entries" {
            const test_allocator = std.testing.allocator;
            const json_src =
                \\[
                \\  { "value": 1 },
                \\  "skip me",
                \\  { "value": 2 }
                \\]
            ;

            var slice_reader = std.Io.Reader.fixed(json_src);
            var json_reader = std.json.Reader.init(test_allocator, &slice_reader);
            defer json_reader.deinit();

            var sum: usize = 0;
            try jsonWalkArrayObjects(test_allocator, &json_reader, &sum, struct {
                fn handle(sum_ptr: *usize, allocator: std.mem.Allocator, reader: *std.json.Reader, _: usize) !void {
                    try jsonWalkObject(allocator, reader, sum_ptr, handleObjectField);
                }

                fn handleObjectField(sum_ptr: *usize, allocator: std.mem.Allocator, reader: *std.json.Reader, key: []const u8) !void {
                    if (std.mem.eql(u8, key, "value")) {
                        sum_ptr.* += try jsonParseU64Value(allocator, reader);
                    } else {
                        try reader.skipValue();
                    }
                }
            }.handle);

            try std.testing.expectEqual(@as(usize, 3), sum);
        }
    };
}
