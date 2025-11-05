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
};

var remote_pricing_loaded = std.atomic.Value(bool).init(false);

pub fn Provider(comptime cfg: ProviderConfig) type {
    const provider_name = cfg.name;
    const sessions_dir_suffix = cfg.sessions_dir_suffix;
    const legacy_fallback_model = cfg.legacy_fallback_model;
    const fallback_pricing = cfg.fallback_pricing;
    const STRATEGY = cfg.strategy;

    return struct {
        const LEGACY_FALLBACK_MODEL = legacy_fallback_model;
        const JSON_EXT = cfg.session_file_ext;
        const FALLBACK_PRICING = fallback_pricing;

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
            summaries: *Model.SummaryBuilder,
            filters: Model.DateFilters,
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
            const before_events = summaries.eventCount();
            try collectEvents(allocator, arena, summaries, filters, events_progress);
            const after_events = summaries.eventCount();
            if (events_progress) |node| std.Progress.Node.end(node);
            std.log.info(
                "{s}.collectEvents produced {d} new events in {d:.2}ms",
                .{ provider_name, after_events - before_events, nsToMs(events_timer.read()) },
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
                "{s}.loadPricing added {d} pricing models (total={d}) in {d:.2}ms",
                .{ provider_name, after_pricing - before_pricing, after_pricing, nsToMs(pricing_timer.read()) },
            );

            std.log.info(
                "{s}.collect completed in {d:.2}ms",
                .{ provider_name, nsToMs(total_timer.read()) },
            );
        }

        fn logSessionWarning(file_path: []const u8, message: []const u8, err: anyerror) void {
            std.log.warn(
                "{s}: {s} '{s}' ({s})",
                .{ provider_name, message, file_path, @errorName(err) },
            );
        }

        fn collectEvents(
            allocator: std.mem.Allocator,
            arena: std.mem.Allocator,
            summaries: *Model.SummaryBuilder,
            filters: Model.DateFilters,
            progress: ?std.Progress.Node,
        ) !void {
            const SharedContext = struct {
                allocator: std.mem.Allocator,
                arena: std.mem.Allocator,
                summaries: *Model.SummaryBuilder,
                builder_mutex: *std.Thread.Mutex,
                filters: Model.DateFilters,
                progress: ?std.Progress.Node,
                files_scanned: *std.atomic.Value(usize),
                files_inflight: *std.atomic.Value(usize),
            };

            const ProcessFn = struct {
                fn run(shared: *SharedContext, path: []u8) void {
                    defer shared.allocator.free(path);
                    defer _ = shared.files_inflight.fetchSub(1, .acq_rel);
                    defer _ = shared.files_scanned.fetchAdd(1, .acq_rel);
                    defer if (shared.progress) |node| std.Progress.Node.completeOne(node);
                    var local_events = std.ArrayListUnmanaged(Model.TokenUsageEvent){};
                    defer local_events.deinit(shared.allocator);

                    const basename = std.fs.path.basename(path);
                    if (basename.len <= JSON_EXT.len or !std.mem.endsWith(u8, basename, JSON_EXT)) return;
                    const session_id_slice = basename[0 .. basename.len - JSON_EXT.len];

                    const maybe_session_id = duplicateNonEmpty(shared.arena, session_id_slice) catch {
                        return;
                    };
                    const session_id = maybe_session_id orelse return;

                    parseSessionFile(shared.allocator, shared.arena, session_id, path, &local_events) catch {
                        return;
                    };

                    if (local_events.items.len == 0) return;

                    shared.builder_mutex.lock();
                    defer shared.builder_mutex.unlock();
                    for (local_events.items) |*event| {
                        shared.summaries.ingest(shared.allocator, shared.arena, event, shared.filters) catch {
                            return;
                        };
                    }
                }
            };

            var timer = try std.time.Timer.start();

            const sessions_dir = resolveSessionsDir(allocator) catch |err| {
                std.log.info("{s}.collectEvents: skipping, unable to resolve sessions dir ({s})", .{ provider_name, @errorName(err) });
                return;
            };
            defer allocator.free(sessions_dir);

            var root_dir = std.fs.openDirAbsolute(sessions_dir, .{ .iterate = true }) catch |err| {
                std.log.info(
                    "{s}.collectEvents: skipping, unable to open sessions dir '{s}' ({s})",
                    .{ provider_name, sessions_dir, @errorName(err) },
                );
                return;
            };
            defer root_dir.close();

            var walker = try root_dir.walk(allocator);
            defer walker.deinit();

            var builder_mutex = std.Thread.Mutex{};

            var thread_safe_arena = std.heap.ThreadSafeAllocator{ .child_allocator = arena };
            var files_scanned = std.atomic.Value(usize).init(0);
            var files_inflight = std.atomic.Value(usize).init(0);
            var shared = SharedContext{
                .allocator = allocator,
                .arena = thread_safe_arena.allocator(),
                .summaries = summaries,
                .builder_mutex = &builder_mutex,
                .filters = filters,
                .progress = progress,
                .files_scanned = &files_scanned,
                .files_inflight = &files_inflight,
            };

            var threaded = std.Io.Threaded.init(allocator);
            defer threaded.deinit();

            const io = threaded.io();
            var group = std.Io.Group.init;

            while (try walker.next()) |entry| {
                if (entry.kind != .file) continue;
                const relative_path = std.mem.sliceTo(entry.path, 0);
                if (!std.mem.endsWith(u8, relative_path, JSON_EXT)) continue;

                const absolute_path = try std.fs.path.join(allocator, &.{ sessions_dir, relative_path });
                _ = shared.files_inflight.fetchAdd(1, .acq_rel);
                group.async(io, ProcessFn.run, .{ &shared, absolute_path });

                if (progress) |node| {
                    const completed = shared.files_scanned.load(.acquire);
                    const inflight = shared.files_inflight.load(.acquire);
                    std.Progress.Node.setEstimatedTotalItems(node, completed + inflight);
                    std.Progress.Node.setCompletedItems(node, completed);
                }
            }

            group.wait(io);
            const final_completed = shared.files_scanned.load(.acquire);
            if (progress) |node| {
                std.Progress.Node.setEstimatedTotalItems(node, final_completed);
                std.Progress.Node.setCompletedItems(node, final_completed);
            }

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
            arena: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
        ) !void {
            switch (STRATEGY) {
                .codex => try parseCodexSessionFile(allocator, arena, session_id, file_path, events),
                .gemini => try parseGeminiSessionFile(allocator, arena, session_id, file_path, events),
                .claude => try parseClaudeSessionFile(allocator, arena, session_id, file_path, events),
            }
        }

        fn parseCodexSessionFile(
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

            var partial_line = std.ArrayListUnmanaged(u8){};
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
                    arena,
                    &scanner,
                    session_id,
                    partial_line.items,
                    events,
                    &previous_totals,
                    &current_model,
                    &current_model_is_fallback,
                );

                if (!newline_consumed) break;
                streamed_total += discard_result;
                if (streamed_total > max_session_size) return;
            }
        }

        fn parseGeminiSessionFile(
            allocator: std.mem.Allocator,
            arena: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
        ) !void {
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
                        if (try duplicateNonEmpty(arena, slice)) |dup| {
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
                        const timestamp_copy = try duplicateNonEmpty(arena, timestamp_slice) orelse continue;
                        const iso_date = timeutil.localIsoDateFromTimestamp(timestamp_copy) catch {
                            continue;
                        };

                        if (msg_obj.get("model")) |model_value| {
                            switch (model_value) {
                                .string => |slice| {
                                    if (try duplicateNonEmpty(arena, slice)) |model_copy| {
                                        current_model = model_copy;
                                        current_model_is_fallback = false;
                                    }
                                },
                                else => {},
                            }
                        }

                        const current_raw = parseGeminiUsage(tokens_obj);
                        const delta = Model.TokenUsage.deltaFrom(current_raw, previous_totals);
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
            arena: std.mem.Allocator,
            session_id: []const u8,
            file_path: []const u8,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
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

            var partial_line = std.ArrayListUnmanaged(u8){};
            defer partial_line.deinit(allocator);

            var session_label = session_id;
            var session_label_overridden = false;
            var current_model: ?[]const u8 = null;
            var current_model_is_fallback = false;
            var streamed_total: usize = 0;
            var line_index: usize = 0;
            var seen_messages = std.AutoHashMap(u64, void).init(allocator);
            defer seen_messages.deinit();

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
                        arena,
                        trimmed,
                        line_index,
                        file_path,
                        &seen_messages,
                        &session_label,
                        &session_label_overridden,
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
            arena: std.mem.Allocator,
            line: []const u8,
            line_index: usize,
            file_path: []const u8,
            seen_messages: *std.AutoHashMap(u64, void),
            session_label: *[]const u8,
            session_label_overridden: *bool,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
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
                            const duplicate = duplicateNonEmpty(arena, slice) catch null;
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
                arena,
                record,
                seen_messages,
                session_label.*,
                events,
                current_model,
                current_model_is_fallback,
            );
        }

        fn emitClaudeEvent(
            allocator: std.mem.Allocator,
            arena: std.mem.Allocator,
            record: std.json.ObjectMap,
            seen_messages: *std.AutoHashMap(u64, void),
            session_label: []const u8,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
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

            if (!try shouldEmitClaudeMessage(session_label, seen_messages, message_obj)) {
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
            const timestamp_copy = duplicateNonEmpty(arena, timestamp_slice) catch return;
            const owned_timestamp = timestamp_copy orelse return;
            const iso_date = timeutil.localIsoDateFromTimestamp(owned_timestamp) catch {
                return;
            };

            var extracted_model: ?[]const u8 = null;
            if (message_obj.get("model")) |model_value| {
                switch (model_value) {
                    .string => |slice| {
                        const duplicated = duplicateNonEmpty(arena, slice) catch null;
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
            };
            try events.append(allocator, event);
        }

        fn shouldEmitClaudeMessage(
            session_label: []const u8,
            seen_messages: *std.AutoHashMap(u64, void),
            message_obj: std.json.ObjectMap,
        ) !bool {
            const id_value = message_obj.get("id") orelse return true;
            const id_slice = switch (id_value) {
                .string => |slice| slice,
                else => return true,
            };
            const session_hash = std.hash.Wyhash.hash(0, session_label);
            const msg_hash = std.hash.Wyhash.hash(session_hash, id_slice);
            const gop = try seen_messages.getOrPut(msg_hash);
            return !gop.found_existing;
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
            arena: std.mem.Allocator,
            scanner: *std.json.Scanner,
            session_id: []const u8,
            line: []const u8,
            events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
            previous_totals: *?RawUsage,
            current_model: *?[]const u8,
            current_model_is_fallback: *bool,
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
                        const duplicated = duplicateNonEmpty(arena, model_token.slice) catch null;
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
            const timestamp_copy = try duplicateNonEmpty(arena, raw_timestamp.slice) orelse {
                raw_timestamp.release(allocator);
                return;
            };
            raw_timestamp.release(allocator);
            const iso_date = timeutil.localIsoDateFromTimestamp(timestamp_copy) catch {
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

            const delta = delta_usage.?;
            if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
                return;
            }

            var extracted_model: ?[]const u8 = null;
            if (payload_result.model) |token| {
                var model_token = token;
                payload_result.model = null;
                if (model_token.slice.len != 0) {
                    extracted_model = try duplicateNonEmpty(arena, model_token.slice);
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

        fn loadPricing(
            arena: std.mem.Allocator,
            allocator: std.mem.Allocator,
            pricing: *Model.PricingMap,
        ) !void {
            var total_timer = try std.time.Timer.start();

            var fetch_attempted = false;
            var fetch_ok = true;
            var fetch_error: ?anyerror = null;
            if (!remote_pricing_loaded.load(.acquire)) {
                fetch_attempted = true;
                var fetch_timer = try std.time.Timer.start();
                const before_fetch = pricing.count();
                fetchRemotePricing(arena, allocator, pricing) catch |err| {
                    fetch_ok = false;
                    fetch_error = err;
                };
                const fetch_elapsed = nsToMs(fetch_timer.read());
                if (fetch_ok) {
                    remote_pricing_loaded.store(true, .release);
                    std.log.info(
                        "{s}.loadPricing: remote pricing fetched in {d:.2}ms (models += {d})",
                        .{ provider_name, fetch_elapsed, pricing.count() - before_fetch },
                    );
                } else {
                    std.log.warn(
                        "{s}.loadPricing: remote pricing failed after {d:.2}ms ({s})",
                        .{ provider_name, fetch_elapsed, @errorName(fetch_error.?) },
                    );
                }
            }

            if (!fetch_attempted) {
                std.log.info("{s}.loadPricing: remote pricing already fetched; skipping", .{provider_name});
            }

            const had_pricing = pricing.count() != 0;
            var fallback_timer = try std.time.Timer.start();
            try ensureFallbackPricing(arena, pricing);
            std.log.info(
                "{s}.loadPricing: {s} fallback pricing in {d:.2}ms (models={d})",
                .{ provider_name, if (had_pricing) "ensured" else "inserted", nsToMs(fallback_timer.read()), pricing.count() },
            );

            std.log.info(
                "{s}.loadPricing completed in {d:.2}ms (models={d})",
                .{ provider_name, nsToMs(total_timer.read()), pricing.count() },
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

            var json_reader = std.json.Reader.init(allocator, body_reader);
            defer json_reader.deinit();

            var parser = PricingFeedParser{
                .arena = arena,
                .allocator = allocator,
                .pricing = pricing,
            };
            try parser.parse(&json_reader);
        }

        fn ensureFallbackPricing(arena: std.mem.Allocator, pricing: *Model.PricingMap) !void {
            for (FALLBACK_PRICING) |fallback| {
                if (pricing.get(fallback.name) != null) continue;
                const key = try arena.dupe(u8, fallback.name);
                try pricing.put(key, fallback.pricing);
            }
        }

        const PricingFeedParser = struct {
            arena: std.mem.Allocator,
            allocator: std.mem.Allocator,
            pricing: *Model.PricingMap,

            pub fn parse(self: *PricingFeedParser, reader: *std.json.Reader) !void {
                var key_buffer = std.array_list.Managed(u8).init(self.allocator);
                defer key_buffer.deinit();
                var scratch = std.array_list.Managed(u8).init(self.allocator);
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
                            const copied_name = try self.arena.dupe(u8, name_slice);
                            const maybe_pricing = try self.parseEntry(reader, &scratch);
                            if (maybe_pricing) |entry| {
                                if (self.pricing.get(copied_name) == null) {
                                    try self.pricing.put(copied_name, entry);
                                }
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
    };
}
