const std = @import("std");
const Model = @import("../model.zig");
const timeutil = @import("../time.zig");
const SessionProvider = @import("session_provider.zig");

const RawUsage = Model.RawTokenUsage;
const CollectWriter = SessionProvider.CollectWriter;
const MessageDeduper = SessionProvider.MessageDeduper;

const Provider = SessionProvider.Provider(.{
    .name = "claude",
    .sessions_dir_suffix = "/.claude/projects",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".jsonl",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseSessionFile,
    .requires_deduper = true,
});

pub const collect = Provider.collect;
pub const loadPricingData = Provider.loadPricingData;

fn parseSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const SessionProvider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*SessionProvider.MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(Model.TokenUsageEvent),
) !void {
    try parseClaudeSessionFile(allocator, ctx, session_id, file_path, deduper, timezone_offset_minutes, events);
}

fn parseClaudeSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const SessionProvider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(Model.TokenUsageEvent),
) !void {
    const max_session_size: usize = 128 * 1024 * 1024;
    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        ctx.logWarning(file_path, "unable to open claude session file", err);
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
            ctx.logWarning(file_path, "error while reading claude session stream", err);
            return;
        };

        var newline_consumed = true;
        const discard_result = reader.discardDelimiterInclusive('\n') catch |err| switch (err) {
            error.EndOfStream => blk: {
                newline_consumed = false;
                break :blk 0;
            },
            else => {
                ctx.logWarning(file_path, "error while advancing claude session stream", err);
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
                ctx,
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
    ctx: *const SessionProvider.ParseContext,
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
            .{ ctx.provider_name, file_path, line_index, @errorName(err) },
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
                    const duplicate = SessionProvider.duplicateNonEmpty(allocator, slice) catch null;
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
        ctx,
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
    ctx: *const SessionProvider.ParseContext,
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
    const timestamp_copy = SessionProvider.duplicateNonEmpty(allocator, timestamp_slice) catch return;
    const owned_timestamp = timestamp_copy orelse return;
    const iso_date = timeutil.isoDateForTimezone(owned_timestamp, timezone_offset_minutes) catch {
        return;
    };

    var extracted_model: ?[]const u8 = null;
    if (message_obj.get("model")) |model_value| {
        switch (model_value) {
            .string => |slice| {
                const duplicated = SessionProvider.duplicateNonEmpty(allocator, slice) catch null;
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
    } else if (ctx.legacy_fallback_model) |legacy| {
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

    if (ctx.legacy_fallback_model) |legacy| {
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
        .display_input_tokens = ctx.computeDisplayInput(usage),
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

test "claude parser emits assistant usage events and respects overrides" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(Model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);

    var deduper = try MessageDeduper.init(worker_allocator);
    defer deduper.deinit();

    const ctx = SessionProvider.ParseContext{
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
