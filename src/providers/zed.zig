const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const RawUsage = model.RawTokenUsage;
const UsageAccumulator = model.UsageAccumulator;
const usageFieldForKey = model.usageFieldForKey;
const parseTokenNumber = model.parseTokenNumber;
const test_helpers = @import("test_helpers.zig");
const testing = std.testing;

const db_path_parts = [_][]const u8{ ".local", "share", "zed", "threads", "threads.db" };
const parse_ctx = provider.ParseContext{
    .provider_name = "zed",
    .legacy_fallback_model = null,
    .cached_counts_overlap_input = false,
};

pub fn collect(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    summaries: *model.SummaryBuilder,
    filters: model.DateFilters,
    progress: ?std.Progress.Node,
) !void {
    var builder_mutex = std.Thread.Mutex{};
    var summary_ctx = struct {
        builder: *model.SummaryBuilder,
    }{ .builder = summaries };

    const consumer = provider.EventConsumer{
        .context = @ptrCast(&summary_ctx),
        .mutex = &builder_mutex,
        .ingest = struct {
            fn ingest(ctx_ptr: *anyopaque, allocator: std.mem.Allocator, event: *const model.TokenUsageEvent, f: model.DateFilters) model.IngestError!void {
                const ctx: *@TypeOf(summary_ctx) = @ptrCast(@alignCast(ctx_ptr));
                try ctx.builder.ingest(allocator, event, f);
            }
        }.ingest,
    };

    try streamEvents(shared_allocator, temp_allocator, filters, consumer, progress);
}

pub fn streamEvents(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    progress: ?std.Progress.Node,
) !void {
    _ = progress;
    const db_path = resolveDbPath(shared_allocator) catch |err| {
        std.log.info("zed: skipping, unable to resolve db path ({s})", .{@errorName(err)});
        return;
    };
    defer shared_allocator.free(db_path);

    const json_rows = runSqliteQuery(temp_allocator, db_path) catch |err| {
        std.log.info("zed: skipping, sqlite3 query failed ({s})", .{@errorName(err)});
        return;
    };
    defer temp_allocator.free(json_rows);

    parseRows(shared_allocator, temp_allocator, filters, consumer, json_rows) catch |err| {
        std.log.warn("zed: failed to parse sqlite output ({s})", .{@errorName(err)});
    };
}

pub fn loadPricingData(shared_allocator: std.mem.Allocator, pricing: *model.PricingMap) !void {
    _ = shared_allocator;
    _ = pricing;
}

fn resolveDbPath(allocator: std.mem.Allocator) ![]u8 {
    const home = std.process.getEnvVarOwned(allocator, "HOME") catch return error.MissingHome;
    defer allocator.free(home);
    var parts: [db_path_parts.len + 1][]const u8 = undefined;
    parts[0] = home;
    for (db_path_parts, 0..) |p, i| parts[i + 1] = p;
    return std.fs.path.join(allocator, &parts);
}

fn runSqliteQuery(allocator: std.mem.Allocator, db_path: []const u8) ![]u8 {
    const query = "select id, updated_at, data_type, hex(data) as data_hex from threads";
    var argv = [_][]const u8{ "sqlite3", "-json", db_path, query };

    var result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &argv,
        .max_output_bytes = 64 * 1024 * 1024,
    }) catch |err| {
        if (err == error.FileNotFound) {
            std.log.err("zed: sqlite3 CLI not found; install sqlite3 to enable Zed ingestion", .{});
        }
        return err;
    };
    defer allocator.free(result.stderr);

    const exit_code: u8 = switch (result.term) {
        .Exited => |code| code,
        else => 255,
    };
    if (exit_code != 0) {
        if (exit_code == 255 and std.mem.find(u8, result.stderr, "not found") != null) {
            std.log.err("zed: sqlite3 CLI not found; install sqlite3 to enable Zed ingestion", .{});
        } else {
            std.log.warn("zed: sqlite3 exited with code {d}: {s}", .{ exit_code, result.stderr });
        }
        allocator.free(result.stdout);
        return error.SqliteFailed;
    }

    return result.stdout;
}

fn parseRows(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    json_payload: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, temp_allocator, json_payload, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .array => |rows| {
            for (rows.items) |row_value| {
                try parseRow(shared_allocator, temp_allocator, filters, consumer, row_value);
            }
        },
        else => return error.InvalidJson,
    }
}

fn parseRow(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    row_value: std.json.Value,
) !void {
    const obj = switch (row_value) {
        .object => |o| o,
        else => return,
    };

    const thread_id = try getObjectString(shared_allocator, obj, "id") orelse return;
    defer shared_allocator.free(thread_id);

    const updated_at = try getObjectString(shared_allocator, obj, "updated_at") orelse return;
    defer shared_allocator.free(updated_at);

    const data_type_owned = try getObjectString(temp_allocator, obj, "data_type");
    const data_type = data_type_owned orelse "zstd";
    defer if (data_type_owned) |dt| temp_allocator.free(dt);

    const data_hex = try getObjectString(temp_allocator, obj, "data_hex") orelse return;
    defer temp_allocator.free(data_hex);

    const blob_len = data_hex.len / 2;
    if (data_hex.len % 2 != 0) {
        std.log.warn("zed: invalid hex length for thread {s} (len={d})", .{ thread_id, data_hex.len });
        return;
    }
    const blob = try temp_allocator.alloc(u8, blob_len);
    errdefer temp_allocator.free(blob);
    _ = std.fmt.hexToBytes(blob, data_hex) catch |err| {
        std.log.warn("zed: invalid hex data for thread {s} ({s})", .{ thread_id, @errorName(err) });
        return;
    };
    defer temp_allocator.free(blob);

    const json_data = decompressIfNeeded(shared_allocator, blob, data_type) catch |err| {
        std.log.warn("zed: decompress failed ({s})", .{@errorName(err)});
        return;
    };
    defer shared_allocator.free(json_data);

    parseThread(shared_allocator, temp_allocator, filters, consumer, thread_id, updated_at, json_data) catch |err| {
        std.log.warn("zed: parse thread failed ({s})", .{@errorName(err)});
    };
}

fn getObjectString(allocator: std.mem.Allocator, obj: std.json.ObjectMap, key: []const u8) !?[]u8 {
    if (obj.get(key)) |val| {
        return switch (val) {
            .string => blk: {
                const dup = try allocator.dupe(u8, val.string);
                break :blk dup;
            },
            else => null,
        };
    }
    return null;
}

test "zed parses sqlite output fixture" {
    const allocator = testing.allocator;
    const count = try test_helpers.runFixtureParse(
        allocator,
        "fixtures/zed/sqlite_output.json",
        parseRows,
    );
    try testing.expect(count > 0);
}

fn decompressIfNeeded(allocator: std.mem.Allocator, blob: []const u8, data_type: []const u8) ![]u8 {
    if (std.mem.eql(u8, data_type, "zstd") or data_type.len == 0) {
        return decompressZstd(allocator, blob);
    }
    return allocator.dupe(u8, blob);
}

fn decompressZstd(allocator: std.mem.Allocator, blob: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    var input_reader: std.Io.Reader = .fixed(blob);
    var dec: std.compress.zstd.Decompress = .init(&input_reader, &.{}, .{ .verify_checksum = true });
    _ = try dec.reader.streamRemaining(&out.writer);

    return out.toOwnedSlice();
}

fn parseThread(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    thread_id: []const u8,
    updated_at: []const u8,
    json_bytes: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, temp_allocator, json_bytes, .{});
    defer parsed.deinit();

    const root = parsed.value;
    const obj = switch (root) {
        .object => |o| o,
        else => return error.InvalidJson,
    };

    var model_name: ?[]const u8 = null;
    defer if (model_name) |m| shared_allocator.free(m);
    if (obj.get("model")) |model_val| {
        model_name = try parseModelValue(shared_allocator, model_val);
    }

    if (obj.get("request_token_usage")) |usage_val| {
        try parseRequestUsageValue(shared_allocator, temp_allocator, filters, consumer, usage_val, thread_id, updated_at, model_name);
    }
}

fn parseModelValue(allocator: std.mem.Allocator, val: std.json.Value) !?[]u8 {
    return switch (val) {
        .object => |o| blk: {
            if (o.get("model")) |mval| {
                if (mval == .string) {
                    const dup = try allocator.dupe(u8, mval.string);
                    break :blk dup;
                }
            }
            break :blk null;
        },
        .string => try allocator.dupe(u8, val.string),
        else => null,
    };
}

fn parseRequestUsageValue(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    val: std.json.Value,
    thread_id: []const u8,
    updated_at: []const u8,
    model_name: ?[]const u8,
) !void {
    const obj = switch (val) {
        .object => |o| o,
        else => return,
    };
    const timestamp_info = (try provider.timestampFromSlice(shared_allocator, updated_at, filters.timezone_offset_minutes)) orelse return;
    defer shared_allocator.free(timestamp_info.text);

    var it = obj.iterator();
    while (it.next()) |entry| {
        const req_id = entry.key_ptr.*;
        try parseUsageEntryValue(shared_allocator, temp_allocator, filters, consumer, req_id, entry.value_ptr.*, thread_id, timestamp_info, model_name);
    }
}

fn parseUsageEntryValue(
    shared_allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    req_id: []const u8,
    usage_val: std.json.Value,
    thread_id: []const u8,
    timestamp_info: provider.TimestampInfo,
    model_name: ?[]const u8,
) !void {
    _ = temp_allocator;
    const obj = switch (usage_val) {
        .object => |o| o,
        else => return,
    };

    var accum = UsageAccumulator{};
    var it = obj.iterator();
    while (it.next()) |entry| {
        const field = usageFieldForKey(entry.key_ptr.*) orelse continue;
        const value = switch (entry.value_ptr.*) {
            .integer => |v| if (v >= 0) @as(u64, @intCast(v)) else 0,
            .float => |v| if (v >= 0) @as(u64, @intFromFloat(std.math.floor(v))) else 0,
            .number_string => |s| parseTokenNumber(s),
            .string => |s| parseTokenNumber(s),
            else => continue,
        };
        accum.addField(field, value);
    }

    const raw = accum.finalize();
    const usage = model.TokenUsage.fromRaw(raw);
    if (!provider.shouldEmitUsage(usage)) return;

    const model_slice = model_name orelse req_id;
    const event = model.TokenUsageEvent{
        .session_id = thread_id,
        .timestamp = timestamp_info.text,
        .local_iso_date = timestamp_info.local_iso_date,
        .model = model_slice,
        .usage = usage,
        .is_fallback = false,
        .display_input_tokens = parse_ctx.computeDisplayInput(usage),
    };

    if (consumer.mutex) |m| m.lock();
    defer if (consumer.mutex) |m| m.unlock();
    try consumer.ingest(consumer.context, shared_allocator, &event, filters);
}
