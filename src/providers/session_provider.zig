const std = @import("std");
const Model = @import("../model.zig");
const timeutil = @import("../time.zig");

pub const FallbackPricingEntry = struct {
    name: []const u8,
    pricing: Model.ModelPricing,
};

pub const ParseStrategy = enum {
    codex,
    gemini,
    claude,
};

pub const ProviderConfig = struct {
    name: []const u8,
    sessions_dir_suffix: []const u8,
    legacy_fallback_model: ?[]const u8 = null,
    fallback_pricing: []const FallbackPricingEntry = &.{},
    session_file_ext: []const u8 = ".jsonl",
    strategy: ParseStrategy = .codex,
    cached_counts_overlap_input: bool = false,
};

var remote_pricing_loaded = std.atomic.Value(bool).init(false);

pub const RemotePricingStats = struct {
    attempted: bool = false,
    models_added: usize = 0,
    elapsed_ms: f64 = 0,
    failure: ?anyerror = null,
};

pub fn loadRemotePricingOnce(
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

    pub fn parse(self: *PricingFeedParser, reader: *std.json.Reader) !void {
        var key_buffer = std.array_list.Managed(u8).init(self.temp_allocator);
        defer key_buffer.deinit();
        var scratch = std.array_list.Managed(u8).init(self.temp_allocator);
        defer scratch.deinit();

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
                    const name_slice = try readStringValue(reader, &key_buffer);
                    const maybe_pricing = try self.parseEntry(reader, &scratch);
                    if (maybe_pricing) |entry| {
                        try self.insertPricingEntries(name_slice, entry);
                    }
                    key_buffer.clearRetainingCapacity();
                },
                else => return error.InvalidResponse,
            }
        }
    }

    fn parseEntry(
        self: *PricingFeedParser,
        reader: *std.json.Reader,
        scratch: *std.array_list.Managed(u8),
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
                    const field = try readStringValue(reader, scratch);
                    if (std.mem.eql(u8, field, "input_cost_per_token")) {
                        input_rate = try readNumber(reader, scratch);
                    } else if (std.mem.eql(u8, field, "output_cost_per_token")) {
                        output_rate = try readNumber(reader, scratch);
                    } else if (std.mem.eql(u8, field, "cache_creation_input_token_cost")) {
                        cache_creation_rate = try readNumber(reader, scratch);
                    } else if (std.mem.eql(u8, field, "cache_read_input_token_cost")) {
                        cached_rate = try readNumber(reader, scratch);
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

    fn insertPricingEntries(self: *PricingFeedParser, name: []const u8, entry: Model.ModelPricing) !void {
        try self.putPricing(name, entry);

        if (std.mem.lastIndexOfScalar(u8, name, '/')) |idx| {
            const alias = name[idx + 1 ..];
            if (alias.len != 0 and !std.mem.eql(u8, alias, name)) {
                try self.putPricing(alias, entry);
            }
        }
    }

    fn putPricing(self: *PricingFeedParser, key: []const u8, entry: Model.ModelPricing) !void {
        if (self.pricing.get(key) != null) return;
        const copied_name = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(copied_name);
        try self.pricing.put(copied_name, entry);
    }
};

fn readStringValue(
    reader: *std.json.Reader,
    buffer: *std.array_list.Managed(u8),
) ![]const u8 {
    buffer.clearRetainingCapacity();
    const slice = try reader.allocNextIntoArrayList(buffer, .alloc_if_needed);
    return slice orelse buffer.items;
}

fn readNumber(
    reader: *std.json.Reader,
    buffer: *std.array_list.Managed(u8),
) !f64 {
    buffer.clearRetainingCapacity();
    const slice = try reader.allocNextIntoArrayList(buffer, .alloc_if_needed);
    const number_slice = slice orelse buffer.items;
    return std.fmt.parseFloat(f64, number_slice) catch return error.InvalidNumber;
}

pub fn Provider(comptime cfg: ProviderConfig) type {
    const provider_name = cfg.name;
    const sessions_dir_suffix = cfg.sessions_dir_suffix;
    const legacy_fallback_model = cfg.legacy_fallback_model;
    const fallback_pricing = cfg.fallback_pricing;
    const STRATEGY = cfg.strategy;
    const CACHED_OVERLAP = cfg.cached_counts_overlap_input;

    return struct {
        pub const EventConsumer = struct {
            context: *anyopaque,
            mutex: ?*std.Thread.Mutex = null,
            ingest: *const fn (*anyopaque, std.mem.Allocator, *Model.TokenUsageEvent, Model.DateFilters) anyerror!void,
        };

        const LEGACY_FALLBACK_MODEL = legacy_fallback_model;
        const JSON_EXT = cfg.session_file_ext;
        const FALLBACK_PRICING = fallback_pricing;

        const RawUsage = Model.RawTokenUsage;
        const TokenString = Model.TokenBuffer;
        const MessageDeduper = struct {
            mutex: std.Thread.Mutex = .{},
            map: std.AutoHashMap(u64, void),

            fn init(allocator: std.mem.Allocator) !MessageDeduper {
                return .{ .map = std.AutoHashMap(u64, void).init(allocator) };
            }

            fn deinit(self: *MessageDeduper) void {
                self.map.deinit();
            }

            fn mark(self: *MessageDeduper, key: u64) !bool {
                self.mutex.lock();
                defer self.mutex.unlock();
                const gop = try self.map.getOrPut(key);
                if (gop.found_existing) return false;
                return true;
            }
        };

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
                    ) catch {
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
            if (STRATEGY == .claude) {
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
            switch (STRATEGY) {
                .codex => try parseCodexSessionFile(allocator, session_id, file_path, deduper, timezone_offset_minutes, events),
                .gemini => try parseGeminiSessionFile(allocator, session_id, file_path, deduper, timezone_offset_minutes, events),
                .claude => try parseClaudeSessionFile(allocator, session_id, file_path, deduper, timezone_offset_minutes, events),
            }
        }

        fn parseCodexSessionFile(
            allocator: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            deduper: ?*MessageDeduper,
            timezone_offset_minutes: i32,
            events: *std.ArrayList(Model.TokenUsageEvent),
        ) !void {
            _ = deduper;
            var previous_totals: ?RawUsage = null;
            var current_model: ?[]const u8 = null;
            var current_model_is_fallback = false;
            const max_session_size: usize = 128 * 1024 * 1024;
            const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
                logSessionWarning(file_path, "unable to open session file", err);
                return;
            };
            defer file.close();

            var io_single = std.Io.Threaded.init_single_threaded;
            defer io_single.deinit();
            const io = io_single.io();

            var reader_buffer: [64 * 1024]u8 = undefined;
            var file_reader = file.readerStreaming(io, reader_buffer[0..]);
            var reader = &file_reader.interface;

            var scanner = std.json.Scanner.initStreaming(allocator);
            defer scanner.deinit();

            var partial_line: std.ArrayList(u8) = .empty;
            defer partial_line.deinit(allocator);

            var streamed_total: usize = 0;

            while (true) {
                partial_line.clearRetainingCapacity();
                var writer_ctx = CollectWriter.init(&partial_line, allocator);
                const streamed = reader.streamDelimiterEnding(&writer_ctx.base, '\n') catch |err| {
                    logSessionWarning(file_path, "error while reading session stream", err);
                    return;
                };

                var newline_consumed = true;
                const discard_result = reader.discardDelimiterInclusive('\n') catch |err| switch (err) {
                    error.EndOfStream => blk: {
                        newline_consumed = false;
                        break :blk 0;
                    },
                    else => {
                        logSessionWarning(file_path, "error while advancing session stream", err);
                        return;
                    },
                };

                if (streamed == 0 and partial_line.items.len == 0 and !newline_consumed) {
                    break;
                }

                streamed_total += streamed;
                if (streamed_total > max_session_size) return;

                try processSessionLine(
                    allocator,
                    &scanner,
                    session_id,
                    partial_line.items,
                    events,
                    &previous_totals,
                    &current_model,
                    &current_model_is_fallback,
                    timezone_offset_minutes,
                );

                if (!newline_consumed) break;
                streamed_total += discard_result;
                if (streamed_total > max_session_size) return;
            }
        }

        fn parseGeminiSessionFile(
            allocator: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            deduper: ?*MessageDeduper,
            timezone_offset_minutes: i32,
            events: *std.ArrayList(Model.TokenUsageEvent),
        ) !void {
            _ = deduper;
            const max_session_size: usize = 32 * 1024 * 1024;
            const file_data = std.fs.cwd().readFileAlloc(file_path, allocator, std.Io.Limit.limited(max_session_size)) catch |err| {
                logSessionWarning(file_path, "failed to read gemini session file", err);
                return;
            };
            defer allocator.free(file_data);

            var parsed = std.json.parseFromSlice(std.json.Value, allocator, file_data, .{}) catch |err| {
                logSessionWarning(file_path, "failed to parse gemini session file", err);
                return;
            };
            defer parsed.deinit();

            const root_value = parsed.value;
            const session_obj = switch (root_value) {
                .object => |obj| obj,
                else => return,
            };

            var session_label = session_id;
            if (session_obj.get("sessionId")) |sid_value| {
                switch (sid_value) {
                    .string => |slice| {
                        if (try duplicateNonEmpty(allocator, slice)) |dup| {
                            session_label = dup;
                        }
                    },
                    else => {},
                }
            }

            const messages_value = session_obj.get("messages") orelse return;
            const messages = switch (messages_value) {
                .array => |arr| arr.items,
                else => return,
            };
            if (messages.len == 0) return;

            var previous_totals: ?RawUsage = null;
            var current_model: ?[]const u8 = null;
            var current_model_is_fallback = false;

            for (messages) |message_value| {
                switch (message_value) {
                    .object => |msg_obj| {
                        const tokens_value = msg_obj.get("tokens") orelse continue;
                        const tokens_obj = switch (tokens_value) {
                            .object => |obj| obj,
                            else => continue,
                        };

                        const timestamp_value = msg_obj.get("timestamp") orelse continue;
                        const timestamp_slice = switch (timestamp_value) {
                            .string => |slice| slice,
                            else => continue,
                        };
                        const timestamp_copy = try duplicateNonEmpty(allocator, timestamp_slice) orelse continue;
                        const iso_date = timeutil.isoDateForTimezone(timestamp_copy, timezone_offset_minutes) catch {
                            continue;
                        };

                        if (msg_obj.get("model")) |model_value| {
                            switch (model_value) {
                                .string => |slice| {
                                    if (try duplicateNonEmpty(allocator, slice)) |model_copy| {
                                        current_model = model_copy;
                                        current_model_is_fallback = false;
                                    }
                                },
                                else => {},
                            }
                        }

                        const current_raw = parseGeminiUsage(tokens_obj);
                        var delta = Model.TokenUsage.deltaFrom(current_raw, previous_totals);
                        normalizeUsageDelta(&delta);
                        previous_totals = current_raw;

                        if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
                            continue;
                        }

                        var model_name = current_model;
                        var is_fallback = current_model_is_fallback;
                        if (model_name == null) {
                            if (LEGACY_FALLBACK_MODEL) |legacy| {
                                model_name = legacy;
                                is_fallback = true;
                                current_model = model_name;
                                current_model_is_fallback = true;
                            } else {
                                continue;
                            }
                        }

                        const event = Model.TokenUsageEvent{
                            .session_id = session_label,
                            .timestamp = timestamp_copy,
                            .local_iso_date = iso_date,
                            .model = model_name.?,
                            .usage = delta,
                            .is_fallback = is_fallback,
                            .display_input_tokens = computeDisplayInput(delta),
                        };
                        try events.append(allocator, event);
                    },
                    else => continue,
                }
            }
        }

        fn parseGeminiUsage(tokens_obj: std.json.ObjectMap) RawUsage {
            return .{
                .input_tokens = jsonValueToU64(tokens_obj.get("input")),
                .cached_input_tokens = jsonValueToU64(tokens_obj.get("cached")),
                .output_tokens = jsonValueToU64(tokens_obj.get("output")) + jsonValueToU64(tokens_obj.get("tool")),
                .reasoning_output_tokens = jsonValueToU64(tokens_obj.get("thoughts")),
                .total_tokens = jsonValueToU64(tokens_obj.get("total")),
            };
        }

        fn jsonValueToU64(maybe_value: ?std.json.Value) u64 {
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

        fn parseClaudeSessionFile(
            allocator: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            deduper: ?*MessageDeduper,
            timezone_offset_minutes: i32,
            events: *std.ArrayList(Model.TokenUsageEvent),
        ) !void {
            const max_session_size: usize = 128 * 1024 * 1024;
            const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
                logSessionWarning(file_path, "unable to open claude session file", err);
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

            var session_label = session_id;
            var session_label_overridden = false;
            var current_model: ?[]const u8 = null;
            var current_model_is_fallback = false;
            var streamed_total: usize = 0;
            var line_index: usize = 0;

            while (true) {
                partial_line.clearRetainingCapacity();
                var writer_ctx = CollectWriter.init(&partial_line, allocator);
                const streamed = reader.streamDelimiterEnding(&writer_ctx.base, '\n') catch |err| {
                    logSessionWarning(file_path, "error while reading claude session stream", err);
                    return;
                };

                var newline_consumed = true;
                const discard_result = reader.discardDelimiterInclusive('\n') catch |err| switch (err) {
                    error.EndOfStream => blk: {
                        newline_consumed = false;
                        break :blk 0;
                    },
                    else => {
                        logSessionWarning(file_path, "error while advancing claude session stream", err);
                        return;
                    },
                };

                if (streamed == 0 and partial_line.items.len == 0 and !newline_consumed) {
                    break;
                }

                streamed_total += streamed;
                if (streamed_total > max_session_size) return;

                const trimmed = std.mem.trim(u8, partial_line.items, " \t\r\n");
                if (trimmed.len != 0) {
                    line_index += 1;
                    try handleClaudeLine(
                        allocator,
                        trimmed,
                        line_index,
                        file_path,
                        deduper,
                        &session_label,
                        &session_label_overridden,
                        timezone_offset_minutes,
                        events,
                        &current_model,
                        &current_model_is_fallback,
                    );
                }

                if (!newline_consumed) break;
                streamed_total += discard_result;
                if (streamed_total > max_session_size) return;
            }
        }

        fn handleClaudeLine(
            allocator: std.mem.Allocator,
            line: []const u8,
            line_index: usize,
            file_path: []const u8,
            deduper: ?*MessageDeduper,
            session_label: *[]const u8,
            session_label_overridden: *bool,
            timezone_offset_minutes: i32,
            events: *std.ArrayList(Model.TokenUsageEvent),
            current_model: *?[]const u8,
            current_model_is_fallback: *bool,
        ) !void {
            var parsed_doc = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch |err| {
                std.log.warn(
                    "{s}: failed to parse claude session file '{s}' line {d} ({s})",
                    .{ provider_name, file_path, line_index, @errorName(err) },
                );
                return;
            };
            defer parsed_doc.deinit();

            const record = switch (parsed_doc.value) {
                .object => |obj| obj,
                else => return,
            };

            if (!session_label_overridden.*) {
                if (record.get("sessionId")) |sid_value| {
                    switch (sid_value) {
                        .string => |slice| {
                            const duplicate = duplicateNonEmpty(allocator, slice) catch null;
                            if (duplicate) |dup| {
                                session_label.* = dup;
                                session_label_overridden.* = true;
                            }
                        },
                        else => {},
                    }
                }
            }

            try emitClaudeEvent(
                allocator,
                record,
                deduper,
                session_label.*,
                timezone_offset_minutes,
                events,
                current_model,
                current_model_is_fallback,
            );
        }

        fn emitClaudeEvent(
            allocator: std.mem.Allocator,
            record: std.json.ObjectMap,
            deduper: ?*MessageDeduper,
            session_label: []const u8,
            timezone_offset_minutes: i32,
            events: *std.ArrayList(Model.TokenUsageEvent),
            current_model: *?[]const u8,
            current_model_is_fallback: *bool,
        ) !void {
            const type_value = record.get("type") orelse return;
            const type_slice = switch (type_value) {
                .string => |slice| slice,
                else => return,
            };
            if (!std.mem.eql(u8, type_slice, "assistant")) return;

            const message_value = record.get("message") orelse return;
            const message_obj = switch (message_value) {
                .object => |obj| obj,
                else => return,
            };

            if (!try shouldEmitClaudeMessage(deduper, record, message_obj)) {
                return;
            }

            const usage_value = message_obj.get("usage") orelse return;
            const usage_obj = switch (usage_value) {
                .object => |obj| obj,
                else => return,
            };

            const timestamp_value = record.get("timestamp") orelse return;
            const timestamp_slice = switch (timestamp_value) {
                .string => |slice| slice,
                else => return,
            };
            const timestamp_copy = duplicateNonEmpty(allocator, timestamp_slice) catch return;
            const owned_timestamp = timestamp_copy orelse return;
            const iso_date = timeutil.isoDateForTimezone(owned_timestamp, timezone_offset_minutes) catch {
                return;
            };

            var extracted_model: ?[]const u8 = null;
            if (message_obj.get("model")) |model_value| {
                switch (model_value) {
                    .string => |slice| {
                        const duplicated = duplicateNonEmpty(allocator, slice) catch null;
                        if (duplicated) |dup| {
                            extracted_model = dup;
                        }
                    },
                    else => {},
                }
            }

            var model_name = extracted_model;
            var is_fallback = false;
            if (model_name) |named| {
                current_model.* = named;
                current_model_is_fallback.* = false;
            } else if (current_model.*) |known| {
                model_name = known;
                is_fallback = current_model_is_fallback.*;
            } else if (LEGACY_FALLBACK_MODEL) |legacy| {
                model_name = legacy;
                is_fallback = true;
                current_model.* = model_name;
                current_model_is_fallback.* = true;
            } else {
                return;
            }

            const raw = parseClaudeUsage(usage_obj);
            const usage = Model.TokenUsage.fromRaw(raw);
            if (usage.input_tokens == 0 and usage.cached_input_tokens == 0 and usage.output_tokens == 0 and usage.reasoning_output_tokens == 0) {
                return;
            }

            if (LEGACY_FALLBACK_MODEL) |legacy| {
                if (std.mem.eql(u8, model_name.?, legacy) and extracted_model == null) {
                    is_fallback = true;
                    current_model_is_fallback.* = true;
                }
            }

            const event = Model.TokenUsageEvent{
                .session_id = session_label,
                .timestamp = owned_timestamp,
                .local_iso_date = iso_date,
                .model = model_name.?,
                .usage = usage,
                .is_fallback = is_fallback,
                .display_input_tokens = computeDisplayInput(usage),
            };
            try events.append(allocator, event);
        }

        fn shouldEmitClaudeMessage(
            deduper: ?*MessageDeduper,
            record: std.json.ObjectMap,
            message_obj: std.json.ObjectMap,
        ) !bool {
            const dedupe = deduper orelse return true;
            const id_value = message_obj.get("id") orelse return true;
            const id_slice = switch (id_value) {
                .string => |slice| slice,
                else => return true,
            };
            const request_value = record.get("requestId") orelse return true;
            const request_slice = switch (request_value) {
                .string => |slice| slice,
                else => return true,
            };
            var hash = std.hash.Wyhash.hash(0, id_slice);
            hash = std.hash.Wyhash.hash(hash, request_slice);
            return try dedupe.mark(hash);
        }

        fn parseClaudeUsage(usage_obj: std.json.ObjectMap) RawUsage {
            const direct_input = jsonValueToU64(usage_obj.get("input_tokens"));
            const cache_creation = jsonValueToU64(usage_obj.get("cache_creation_input_tokens"));
            const cached_reads = jsonValueToU64(usage_obj.get("cache_read_input_tokens"));
            const output_tokens = jsonValueToU64(usage_obj.get("output_tokens"));

            const input_total = std.math.add(u64, direct_input, cache_creation) catch std.math.maxInt(u64);
            const with_cached = std.math.add(u64, input_total, cached_reads) catch std.math.maxInt(u64);
            const total_tokens = std.math.add(u64, with_cached, output_tokens) catch std.math.maxInt(u64);

            return .{
                .input_tokens = direct_input,
                .cache_creation_input_tokens = cache_creation,
                .cached_input_tokens = cached_reads,
                .output_tokens = output_tokens,
                .reasoning_output_tokens = 0,
                .total_tokens = total_tokens,
            };
        }

        fn normalizeUsageDelta(delta: *Model.TokenUsage) void {
            if (!CACHED_OVERLAP) return;
            const overlap = if (delta.cached_input_tokens > delta.input_tokens)
                delta.input_tokens
            else
                delta.cached_input_tokens;
            delta.input_tokens -= overlap;
            delta.cached_input_tokens = overlap;
        }

        fn computeDisplayInput(usage: Model.TokenUsage) u64 {
            if (!CACHED_OVERLAP) return usage.input_tokens;
            return std.math.add(u64, usage.input_tokens, usage.cached_input_tokens) catch std.math.maxInt(u64);
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

        fn processSessionLine(
            allocator: std.mem.Allocator,
            scanner: *std.json.Scanner,
            session_id: []const u8,
            line: []const u8,
            events: *std.ArrayList(Model.TokenUsageEvent),
            previous_totals: *?RawUsage,
            current_model: *?[]const u8,
            current_model_is_fallback: *bool,
            timezone_offset_minutes: i32,
        ) !void {
            const trimmed = std.mem.trim(u8, line, " \t\r\n");
            if (trimmed.len == 0) return;

            resetScanner(scanner, trimmed);

            const start_token = scanner.next() catch {
                return;
            };
            if (start_token != .object_begin) return;

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
                            scanner,
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
                            scanner,
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

            if (parse_failed) return;

            _ = scanner.next() catch {};

            if (timestamp_token == null) return;

            if (is_turn_context) {
                if (payload_result.model) |token| {
                    var model_token = token;
                    payload_result.model = null;
                    if (model_token.slice.len != 0) {
                        const duplicated = duplicateNonEmpty(allocator, model_token.slice) catch null;
                        if (duplicated) |model_copy| {
                            current_model.* = model_copy;
                            current_model_is_fallback.* = false;
                        }
                    }
                    model_token.release(allocator);
                }
                return;
            }

            if (!is_event_msg) return;

            var payload_type_is_token_count = false;
            if (payload_result.payload_type) |token| {
                payload_type_is_token_count = std.mem.eql(u8, token.slice, "token_count");
            }
            if (!payload_type_is_token_count) return;

            var raw_timestamp = timestamp_token.?;
            timestamp_token = null;
            const timestamp_copy = try duplicateNonEmpty(allocator, raw_timestamp.slice) orelse {
                raw_timestamp.release(allocator);
                return;
            };
            raw_timestamp.release(allocator);
            const iso_date = timeutil.isoDateForTimezone(timestamp_copy, timezone_offset_minutes) catch {
                return;
            };

            var delta_usage: ?Model.TokenUsage = null;
            if (payload_result.last_usage) |last_usage| {
                delta_usage = Model.TokenUsage.fromRaw(last_usage);
            } else if (payload_result.total_usage) |total_usage| {
                delta_usage = Model.TokenUsage.deltaFrom(total_usage, previous_totals.*);
            }
            if (payload_result.total_usage) |total_usage| {
                previous_totals.* = total_usage;
            }

            if (delta_usage == null) return;

            var delta = delta_usage.?;
            normalizeUsageDelta(&delta);
            if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
                return;
            }

            var extracted_model: ?[]const u8 = null;
            if (payload_result.model) |token| {
                var model_token = token;
                payload_result.model = null;
                if (model_token.slice.len != 0) {
                    extracted_model = try duplicateNonEmpty(allocator, model_token.slice);
                    if (extracted_model == null) extracted_model = current_model.*;
                }
                model_token.release(allocator);
            }

            var model_name = extracted_model;
            var is_fallback = false;
            if (model_name == null) {
                if (current_model.*) |known| {
                    model_name = known;
                    is_fallback = current_model_is_fallback.*;
                } else if (LEGACY_FALLBACK_MODEL) |legacy| {
                    model_name = legacy;
                    is_fallback = true;
                    current_model.* = model_name;
                    current_model_is_fallback.* = true;
                }
            } else {
                current_model.* = model_name;
                current_model_is_fallback.* = false;
            }

            if (LEGACY_FALLBACK_MODEL == null and model_name == null) {
                return;
            }
            if (LEGACY_FALLBACK_MODEL) |legacy| {
                if (std.mem.eql(u8, model_name.?, legacy) and extracted_model == null) {
                    is_fallback = true;
                    current_model_is_fallback.* = true;
                }
            }

            const event = Model.TokenUsageEvent{
                .session_id = session_id,
                .timestamp = timestamp_copy,
                .local_iso_date = iso_date,
                .model = model_name.?,
                .usage = delta,
                .is_fallback = is_fallback,
                .display_input_tokens = computeDisplayInput(delta),
            };
            try events.append(allocator, event);
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

        fn ensureFallbackPricing(shared_allocator: std.mem.Allocator, pricing: *Model.PricingMap) !void {
            for (FALLBACK_PRICING) |fallback| {
                if (pricing.get(fallback.name) != null) continue;
                const key = try shared_allocator.dupe(u8, fallback.name);
                try pricing.put(key, fallback.pricing);
            }
        }

        fn duplicateNonEmpty(arena: std.mem.Allocator, value: []const u8) !?[]const u8 {
            const trimmed = std.mem.trim(u8, value, " \t\r\n");
            if (trimmed.len == 0) return null;
            return try arena.dupe(u8, trimmed);
        }

        const CollectWriter = struct {
            base: std.Io.Writer,
            list: *std.ArrayList(u8),
            allocator: std.mem.Allocator,

            fn init(list: *std.ArrayList(u8), allocator: std.mem.Allocator) CollectWriter {
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

        test "codex parser emits usage events from token_count entries" {
            const allocator = std.testing.allocator;
            var arena_state = std.heap.ArenaAllocator.init(allocator);
            defer arena_state.deinit();
            const worker_allocator = arena_state.allocator();

            const CodexProvider = Provider(.{
                .name = "codex-test",
                .sessions_dir_suffix = "/unused",
                .legacy_fallback_model = "gpt-5",
                .cached_counts_overlap_input = true,
            });

            var events = std.ArrayList(Model.TokenUsageEvent){};
            defer events.deinit(worker_allocator);

            try CodexProvider.parseCodexSessionFile(
                worker_allocator,
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

        test "gemini parser converts message totals into usage deltas" {
            const allocator = std.testing.allocator;
            var arena_state = std.heap.ArenaAllocator.init(allocator);
            defer arena_state.deinit();
            const worker_allocator = arena_state.allocator();

            const GeminiProvider = Provider(.{
                .name = "gemini-test",
                .sessions_dir_suffix = "/unused",
                .session_file_ext = ".json",
                .strategy = .gemini,
                .cached_counts_overlap_input = false,
            });

            var events: std.ArrayList(Model.TokenUsageEvent) = .empty;
            defer events.deinit(worker_allocator);

            try GeminiProvider.parseGeminiSessionFile(
                worker_allocator,
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

        test "claude parser emits assistant usage events and respects overrides" {
            const allocator = std.testing.allocator;
            var arena_state = std.heap.ArenaAllocator.init(allocator);
            defer arena_state.deinit();
            const worker_allocator = arena_state.allocator();

            const ClaudeProvider = Provider(.{
                .name = "claude-test",
                .sessions_dir_suffix = "/unused",
                .strategy = .claude,
            });

            var events: std.ArrayList(Model.TokenUsageEvent) = .empty;
            defer events.deinit(worker_allocator);

            try ClaudeProvider.parseClaudeSessionFile(
                worker_allocator,
                "claude-fixture",
                "fixtures/claude/basic.jsonl",
                null,
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

        test "pricing parser stores alias for slash-separated model names" {
            const allocator = std.testing.allocator;
            var pricing = Model.PricingMap.init(allocator);
            defer Model.deinitPricingMap(&pricing, allocator);

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
            try parser.insertPricingEntries("gemini/gemini-flash-latest", pricing_entry);

            try std.testing.expect(pricing.get("gemini/gemini-flash-latest") != null);
            try std.testing.expect(pricing.get("gemini-flash-latest") != null);
        }
    };
}
