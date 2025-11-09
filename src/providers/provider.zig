const std = @import("std");
const Model = @import("../model.zig");
const timeutil = @import("../time.zig");

pub const FallbackPricingEntry = struct {
    name: []const u8,
    pricing: Model.ModelPricing,
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

    pub fn normalizeUsageDelta(self: ParseContext, delta: *Model.TokenUsage) void {
        if (!self.cached_counts_overlap_input) return;
        const overlap = if (delta.cached_input_tokens > delta.input_tokens)
            delta.input_tokens
        else
            delta.cached_input_tokens;
        delta.input_tokens -= overlap;
        delta.cached_input_tokens = overlap;
    }

    pub fn computeDisplayInput(self: ParseContext, usage: Model.TokenUsage) u64 {
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
            if (T == Model.TokenBuffer) {
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

pub const CollectWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(list: *std.ArrayList(u8), allocator: std.mem.Allocator) CollectWriter {
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

    pub fn drain(
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

    pub fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
        return;
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
    if (state.current) |model| {
        return .{ .name = model, .is_fallback = state.is_fallback };
    }

    if (ctx.legacy_fallback_model) |legacy| {
        state.current = legacy;
        state.is_fallback = true;
        return .{ .name = legacy, .is_fallback = true };
    }

    return null;
}

pub fn jsonValueToU64(maybe_value: ?std.json.Value) u64 {
    const value = maybe_value orelse return 0;
    return switch (value) {
        .integer => |val| if (val >= 0) @as(u64, @intCast(val)) else 0,
        .float => |val| if (val >= 0)
            std.math.lossyCast(u64, @floor(val))
        else
            0,
        .number_string => |slice| Model.parseTokenNumber(slice),
        else => 0,
    };
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

pub fn timestampFromValue(
    allocator: std.mem.Allocator,
    timezone_offset_minutes: i32,
    maybe_value: ?std.json.Value,
) !?TimestampInfo {
    const value = maybe_value orelse return null;
    return switch (value) {
        .string => |slice| try timestampFromSlice(allocator, slice, timezone_offset_minutes),
        else => null,
    };
}

pub fn overrideSessionLabelFromValue(
    allocator: std.mem.Allocator,
    session_label: *[]const u8,
    overridden: ?*bool,
    maybe_value: ?std.json.Value,
) void {
    if (overridden) |flag| if (flag.*) return;
    const value = maybe_value orelse return;
    const slice = switch (value) {
        .string => |str| str,
        else => return,
    };
    const duplicate = duplicateNonEmpty(allocator, slice) catch null;
    if (duplicate) |dup| {
        session_label.* = dup;
        if (overridden) |flag| flag.* = true;
    }
}

pub const JsonFileOptions = struct {
    limit: std.Io.Limit = std.Io.Limit.limited(32 * 1024 * 1024),
    read_error_message: []const u8 = "failed to read session file",
    parse_error_message: []const u8 = "failed to parse session file",
};

pub fn readJsonValue(
    allocator: std.mem.Allocator,
    ctx: *const ParseContext,
    file_path: []const u8,
    options: JsonFileOptions,
) ?std.json.Parsed(std.json.Value) {
    const file_data = std.fs.cwd().readFileAlloc(file_path, allocator, options.limit) catch |err| {
        ctx.logWarning(file_path, options.read_error_message, err);
        return null;
    };
    defer allocator.free(file_data);

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, file_data, .{}) catch |err| {
        ctx.logWarning(file_path, options.parse_error_message, err);
        return null;
    };
    return parsed;
}

pub const UsageValueMode = enum {
    set,
    add,
};

pub const UsageFieldDescriptor = struct {
    key: []const u8,
    field: Model.UsageField,
    mode: UsageValueMode = .set,
};

pub fn parseUsageObject(
    usage_obj: std.json.ObjectMap,
    descriptors: []const UsageFieldDescriptor,
) Model.RawTokenUsage {
    var accumulator = Model.UsageAccumulator{};
    for (descriptors) |descriptor| {
        const value = jsonValueToU64(usage_obj.get(descriptor.key));
        switch (descriptor.mode) {
            .set => accumulator.applyField(descriptor.field, value),
            .add => accumulator.addField(descriptor.field, value),
        }
    }
    return accumulator.finalize();
}

pub const JsonlStreamOptions = struct {
    max_bytes: usize = 128 * 1024 * 1024,
    delimiter: u8 = '\n',
    trim_lines: bool = true,
    skip_empty: bool = true,
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

    while (true) {
        partial_line.clearRetainingCapacity();
        var writer_ctx = CollectWriter.init(&partial_line, allocator);
        const streamed = reader.streamDelimiterEnding(&writer_ctx.base, options.delimiter) catch |err| {
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
        if (streamed_total > options.max_bytes) return;

        var line_slice: []const u8 = partial_line.items;
        if (options.trim_lines) {
            line_slice = std.mem.trim(u8, line_slice, " \t\r\n");
        }
        if (options.skip_empty and line_slice.len == 0) {
            if (!newline_consumed) break;
            streamed_total += discard_result;
            if (streamed_total > options.max_bytes) return;
            continue;
        }

        line_index += 1;
        try handler(handler_context, line_slice, line_index);

        if (!newline_consumed) break;
        streamed_total += discard_result;
        if (streamed_total > options.max_bytes) return;
    }
}

pub const ParseSessionFn = *const fn (
    allocator: std.mem.Allocator,
    ctx: *const ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(Model.TokenUsageEvent),
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
        pub const EventConsumer = ProviderType.EventConsumer;
    };
}

var remote_pricing_loaded = std.atomic.Value(bool).init(false);

pub const RemotePricingStats = struct {
    attempted: bool = false,
    models_added: usize = 0,
    elapsed_ms: f64 = 0,
    failure: ?anyerror = null,
};

pub fn loadRemotePricing(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !RemotePricingStats {
    var stats: RemotePricingStats = .{};
    if (remote_pricing_loaded.load(.acquire)) return stats;

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
        remote_pricing_loaded.store(true, .release);
    }
    return stats;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn fetchRemotePricing(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !void {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();

    var client = std.http.Client{
        .allocator = temp_allocator,
        .io = io_single.io(),
    };
    defer client.deinit();

    const uri = try std.Uri.parse(Model.PRICING_URL);
    var request = try client.request(.GET, uri, .{
        .headers = .{ .accept_encoding = .{ .override = "identity" } },
    });
    defer request.deinit();
    try request.sendBodiless();

    var response = try request.receiveHead(&.{});
    if (response.head.status.class() != .success) return error.HttpError;

    var transfer_buffer: [4096]u8 = undefined;
    const body_reader = response.reader(&transfer_buffer);

    var json_reader = std.json.Reader.init(temp_allocator, body_reader);
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
    pricing: *Model.PricingMap,
    const AliasError = error{OutOfMemory};

    pub fn parse(self: *PricingFeedParser, reader: *std.json.Reader) !void {
        var alias_buffer: std.ArrayList(u8) = .empty;
        defer alias_buffer.deinit(self.temp_allocator);

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
                        try self.insertPricingEntries(name.view(), entry, &alias_buffer);
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
    ) !?Model.ModelPricing {
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
        return Model.ModelPricing{
            .input_cost_per_m = input_rate.? * Model.MILLION,
            .cache_creation_cost_per_m = creation * Model.MILLION,
            .cached_input_cost_per_m = cached * Model.MILLION,
            .output_cost_per_m = output_rate.? * Model.MILLION,
        };
    }

    fn insertPricingEntries(
        self: *PricingFeedParser,
        name: []const u8,
        entry: Model.ModelPricing,
        alias_buffer: *std.ArrayList(u8),
    ) AliasError!void {
        _ = alias_buffer;
        _ = try self.putPricing(name, entry);
    }

    fn putPricing(self: *PricingFeedParser, key: []const u8, entry: Model.ModelPricing) AliasError!bool {
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
        pub const EventConsumer = struct {
            context: *anyopaque,
            mutex: ?*std.Thread.Mutex = null,
            ingest: *const fn (*anyopaque, std.mem.Allocator, *Model.TokenUsageEvent, Model.DateFilters) anyerror!void,
        };

        const PARSE_CONTEXT = ParseContext{
            .provider_name = provider_name,
            .legacy_fallback_model = legacy_fallback_model,
            .cached_counts_overlap_input = cfg.cached_counts_overlap_input,
        };
        const PARSE_FN = cfg.parse_session_fn;
        const JSON_EXT = cfg.session_file_ext;
        const FALLBACK_PRICING = fallback_pricing;
        const REQUIRES_DEDUPER = cfg.requires_deduper;

        const SummaryConsumer = struct {
            builder: *Model.SummaryBuilder,
        };

        fn summaryIngest(
            ctx_ptr: *anyopaque,
            allocator: std.mem.Allocator,
            event: *Model.TokenUsageEvent,
            filters: Model.DateFilters,
        ) anyerror!void {
            const ctx = @as(*SummaryConsumer, @ptrCast(@alignCast(ctx_ptr)));
            try ctx.builder.ingest(allocator, event, filters);
        }

        pub fn collect(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            summaries: *Model.SummaryBuilder,
            filters: Model.DateFilters,
            _pricing: *Model.PricingMap,
            progress: ?std.Progress.Node,
        ) !void {
            var total_timer = try std.time.Timer.start();
            _ = _pricing;

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
            std.log.info(
                "{s}.collectEvents produced {d} new events in {d:.2}ms",
                .{ provider_name, after_events - before_events, nsToMs(events_timer.read()) },
            );

            std.log.info(
                "{s}.collect completed in {d:.2}ms",
                .{ provider_name, nsToMs(total_timer.read()) },
            );
        }

        pub fn streamEvents(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            filters: Model.DateFilters,
            consumer: EventConsumer,
        ) !void {
            try collectEvents(shared_allocator, temp_allocator, filters, consumer, null);
        }

        pub fn loadPricingData(
            shared_allocator: std.mem.Allocator,
            temp_allocator: std.mem.Allocator,
            pricing: *Model.PricingMap,
        ) !void {
            _ = temp_allocator;
            try ensureFallbackPricing(shared_allocator, pricing);
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
            filters: Model.DateFilters,
            consumer: EventConsumer,
            progress: ?std.Progress.Node,
        ) !void {
            const SharedContext = struct {
                shared_allocator: std.mem.Allocator,
                temp_allocator: std.mem.Allocator,
                filters: Model.DateFilters,
                progress: ?std.Progress.Node,
                sessions_dir: []const u8,
                paths: []const []const u8,
                completed: *std.atomic.Value(usize),
                deduper: ?*MessageDeduper,
                consumer: EventConsumer,
            };

            const ProcessArgs = struct {
                index: usize,
            };

            const ProcessFn = struct {
                fn run(shared: *SharedContext, args: ProcessArgs) void {
                    const previous = shared.completed.fetchAdd(1, .acq_rel);
                    defer if (shared.progress) |node| node.setCompletedItems(previous + 1);

                    var worker_arena_state = std.heap.ArenaAllocator.init(shared.temp_allocator);
                    defer worker_arena_state.deinit();
                    const worker_allocator = worker_arena_state.allocator();

                    const timezone_offset = @as(i32, shared.filters.timezone_offset_minutes);
                    var local_events: std.ArrayList(Model.TokenUsageEvent) = .empty;
                    defer local_events.deinit(worker_allocator);

                    const relative = shared.paths[args.index];
                    const absolute_path = std.fs.path.join(worker_allocator, &.{ shared.sessions_dir, relative }) catch |err| {
                        std.log.warn("{s}.collectEvents: unable to build path for '{s}' ({s})", .{
                            provider_name,
                            relative,
                            @errorName(err),
                        });
                        return;
                    };
                    defer worker_allocator.free(absolute_path);

                    if (relative.len <= JSON_EXT.len or !std.mem.endsWith(u8, relative, JSON_EXT)) return;
                    const session_id_slice = relative[0 .. relative.len - JSON_EXT.len];

                    const maybe_session_id = duplicateNonEmpty(worker_allocator, session_id_slice) catch {
                        return;
                    };
                    const session_id = maybe_session_id orelse return;

                    parseSessionFile(
                        worker_allocator,
                        session_id,
                        absolute_path,
                        shared.deduper,
                        timezone_offset,
                        &local_events,
                    ) catch |err| {
                        logSessionWarning(absolute_path, "failed to parse session file", err);
                        return;
                    };

                    if (local_events.items.len == 0) return;

                    if (shared.consumer.mutex) |mutex| mutex.lock();
                    defer if (shared.consumer.mutex) |mutex| mutex.unlock();
                    for (local_events.items) |*event| {
                        shared.consumer.ingest(shared.consumer.context, shared.shared_allocator, event, shared.filters) catch {
                            return;
                        };
                    }
                }
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
                if (!std.mem.endsWith(u8, relative_path, JSON_EXT)) continue;
                const copy = try shared_allocator.dupe(u8, relative_path);
                try relative_paths.append(shared_allocator, copy);
            }

            if (relative_paths.items.len == 0) {
                if (progress) |node| {
                    std.Progress.Node.setEstimatedTotalItems(node, 0);
                    std.Progress.Node.setCompletedItems(node, 0);
                }
                std.log.info("{s}.collectEvents: scanned 0 files in {d:.2}ms", .{ provider_name, nsToMs(timer.read()) });
                return;
            }

            var completed = std.atomic.Value(usize).init(0);
            if (progress) |node| {
                std.Progress.Node.setEstimatedTotalItems(node, relative_paths.items.len);
                std.Progress.Node.setCompletedItems(node, 0);
            }

            var deduper_storage: ?MessageDeduper = null;
            defer if (deduper_storage) |*ded| ded.deinit();
            if (REQUIRES_DEDUPER) {
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

            var threaded: std.Io.Threaded = .init(temp_allocator);
            defer threaded.deinit();

            const io = threaded.io();
            var group = std.Io.Group.init;

            for (relative_paths.items, 0..) |_, idx| {
                group.async(io, ProcessFn.run, .{ &shared, .{ .index = idx } });
            }

            group.wait(io);
            const final_completed = completed.load(.acquire);

            std.log.info(
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
            events: *std.ArrayList(Model.TokenUsageEvent),
        ) !void {
            try PARSE_FN(allocator, &PARSE_CONTEXT, session_id, file_path, deduper, timezone_offset_minutes, events);
        }

        fn ensureFallbackPricing(shared_allocator: std.mem.Allocator, pricing: *Model.PricingMap) !void {
            for (FALLBACK_PRICING) |fallback| {
                if (pricing.get(fallback.name) != null) continue;
                const key = try shared_allocator.dupe(u8, fallback.name);
                try pricing.put(key, fallback.pricing);
            }
        }

        test "pricing parser stores manifest entries" {
            const allocator = std.testing.allocator;
            var pricing = Model.PricingMap.init(allocator);
            defer Model.deinitPricingMap(&pricing, allocator);
            var alias_buffer: std.ArrayList(u8) = .empty;
            defer alias_buffer.deinit(allocator);

            var parser = PricingFeedParser{
                .allocator = allocator,
                .temp_allocator = allocator,
                .pricing = &pricing,
            };

            const pricing_entry = Model.ModelPricing{
                .input_cost_per_m = 1,
                .cache_creation_cost_per_m = 1,
                .cached_input_cost_per_m = 1,
                .output_cost_per_m = 1,
            };
            try parser.insertPricingEntries("gemini/gemini-flash-latest", pricing_entry, &alias_buffer);

            try std.testing.expect(pricing.get("gemini/gemini-flash-latest") != null);
        }

        test "usage descriptors map and accumulate token fields" {
            const allocator = std.testing.allocator;
            const json_src =
                \\{
                \\  "input_direct": 150,
                \\  "cache_read": 40,
                \\  "output_primary": 25,
                \\  "output_tool": 5,
                \\  "thoughts": 7
                \\}
            ;

            var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_src, .{});
            defer parsed.deinit();
            const usage_obj = switch (parsed.value) {
                .object => |obj| obj,
                else => unreachable,
            };

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

            const usage = parseUsageObject(usage_obj, descriptors[0..]);
            try std.testing.expectEqual(@as(u64, 150), usage.input_tokens);
            try std.testing.expectEqual(@as(u64, 40), usage.cached_input_tokens);
            try std.testing.expectEqual(@as(u64, 30), usage.output_tokens);
            try std.testing.expectEqual(@as(u64, 7), usage.reasoning_output_tokens);
            try std.testing.expectEqual(@as(u64, 220), usage.total_tokens);
        }

        test "timestamp helpers duplicate json values" {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const json_value = std.json.Value{ .string = "2025-02-15T09:30:00Z" };
            const info = try timestampFromValue(allocator, 0, json_value) orelse unreachable;
            try std.testing.expectEqualStrings("2025-02-15", info.local_iso_date[0..]);
        }

        test "session label overrides respect guard" {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            var label: []const u8 = "default";
            var overridden = false;
            const first = std.json.Value{ .string = "override-one" };
            overrideSessionLabelFromValue(allocator, &label, &overridden, first);
            try std.testing.expectEqualStrings("override-one", label);
            try std.testing.expect(overridden);

            const second = std.json.Value{ .string = "override-two" };
            overrideSessionLabelFromValue(allocator, &label, &overridden, second);
            try std.testing.expectEqualStrings("override-one", label);
        }

        test "readJsonValue loads file data" {
            const allocator = std.testing.allocator;
            const tmp_path = "tokenuze-json-test.json";
            defer std.fs.cwd().deleteFile(tmp_path) catch {};
            try std.fs.cwd().writeFile(.{
                .sub_path = tmp_path,
                .data = "{ \"ok\": true }",
            });

            const ctx = ParseContext{
                .provider_name = "test",
                .legacy_fallback_model = null,
                .cached_counts_overlap_input = false,
            };

            const parsed_opt = readJsonValue(
                allocator,
                &ctx,
                tmp_path,
                .{},
            ) orelse unreachable;
            var parsed = parsed_opt;
            defer parsed.deinit();
            switch (parsed.value) {
                .object => |obj| try std.testing.expect(obj.get("ok") != null),
                else => unreachable,
            }
        }
    };
}
