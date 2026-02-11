const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const builtin = @import("builtin");

const Context = @import("../Context.zig");
const model = @import("../model.zig");
const UsageAccumulator = model.UsageAccumulator;
const usageFieldForKey = model.usageFieldForKey;
const parseTokenNumber = model.parseTokenNumber;
const provider = @import("provider.zig");
const test_helpers = @import("test_helpers.zig");

const log = std.log.scoped(.zed);

const linux_parts = [_][]const u8{ ".local", "share", "zed", "threads", "threads.db" };
const mac_parts = [_][]const u8{ "Library", "Application Support", "Zed", "threads", "threads.db" };
const windows_parts = [_][]const u8{ "Zed", "threads", "threads.db" };

pub fn collect(
    ctx: Context,
    summaries: *model.SummaryBuilder,
    filters: model.DateFilters,
    progress: ?std.Progress.Node,
) !void {
    var builder_mutex: std.Io.Mutex = .init;
    var summary_ctx = struct {
        builder: *model.SummaryBuilder,
    }{ .builder = summaries };

    const consumer = provider.EventConsumer{
        .context = @ptrCast(&summary_ctx),
        .mutex = &builder_mutex,
        .ingest = struct {
            fn ingest(ctx_ptr: *anyopaque, allocator: std.mem.Allocator, event: *const model.TokenUsageEvent, f: model.DateFilters) model.IngestError!void {
                const summary: *@TypeOf(summary_ctx) = @ptrCast(@alignCast(ctx_ptr));
                try summary.builder.ingest(allocator, event, f);
            }
        }.ingest,
    };

    try streamEvents(ctx, filters, consumer, progress);
}

pub fn streamEvents(
    ctx: Context,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    progress: ?std.Progress.Node,
) !void {
    _ = progress;
    const db_path = resolveDbPath(ctx) catch |err| {
        log.info("skipping, unable to resolve db path ({s})", .{@errorName(err)});
        return;
    };
    defer ctx.allocator.free(db_path);

    const json_rows = runSqliteQuery(ctx, db_path) catch |err| {
        log.info("skipping, sqlite3 query failed ({s})", .{@errorName(err)});
        return;
    };
    defer ctx.temp_allocator.free(json_rows);

    parseRows(ctx, filters, consumer, json_rows) catch |err| {
        log.warn("failed to parse sqlite output ({s})", .{@errorName(err)});
    };
}

pub fn loadPricingData(ctx: Context, pricing: *model.PricingMap) !void {
    _ = ctx;
    _ = pricing;
}

pub fn pathHint(ctx: Context) ![]u8 {
    return resolveDbPath(ctx);
}

fn resolveDbPath(ctx: Context) ![]u8 {
    const os_tag = builtin.target.os.tag;
    switch (os_tag) {
        .windows => {
            const local_app_data = ctx.environ_map.get("LOCALAPPDATA") orelse return error.MissingLocalAppData;

            var parts: [windows_parts.len + 1][]const u8 = undefined;
            parts[0] = local_app_data;
            for (windows_parts, 0..) |p, i| parts[i + 1] = p;
            return std.fs.path.join(ctx.allocator, &parts);
        },
        .macos, .linux => {
            const home = ctx.environ_map.get("HOME") orelse return error.MissingHome;

            const sub_parts = if (os_tag == .macos) &mac_parts else &linux_parts;
            comptime assert(mac_parts.len == linux_parts.len);

            var parts: [mac_parts.len + 1][]const u8 = undefined;
            parts[0] = home;
            for (sub_parts, 0..) |p, i| parts[i + 1] = p;
            return std.fs.path.join(ctx.allocator, &parts);
        },
        else => @compileError("zed provider does not support this OS"),
    }
}

fn runSqliteQuery(ctx: Context, db_path: []const u8) ![]u8 {
    const query = "select id, updated_at, data_type, hex(data) as data_hex from threads";
    var argv = [_][]const u8{ "sqlite3", "-json", db_path, query };

    var result = std.process.run(ctx.temp_allocator, ctx.io, .{
        .argv = argv[0..],
        .environ_map = ctx.environ_map,
        .stdout_limit = .limited(64 * 1024 * 1024),
        .stderr_limit = .limited(64 * 1024 * 1024),
    }) catch |err| {
        if (err == error.FileNotFound) {
            log.err("sqlite3 CLI not found; install sqlite3 to enable Zed ingestion", .{});
        }
        return err;
    };
    defer ctx.temp_allocator.free(result.stderr);

    const exit_code: u8 = switch (result.term) {
        .exited => |code| code,
        else => 255,
    };
    if (exit_code != 0) {
        if (exit_code == 255 and std.mem.find(u8, result.stderr, "not found") != null) {
            log.err("sqlite3 CLI not found; install sqlite3 to enable Zed ingestion", .{});
        } else {
            log.warn("sqlite3 exited with code {d}: {s}", .{ exit_code, result.stderr });
        }
        ctx.temp_allocator.free(result.stdout);
        return error.SqliteFailed;
    }

    return result.stdout;
}

fn parseRows(ctx: Context, filters: model.DateFilters, consumer: provider.EventConsumer, json_payload: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.temp_allocator, json_payload, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .array => |rows| {
            for (rows.items) |row_value| {
                try parseRow(ctx, filters, consumer, row_value);
            }
        },
        else => return error.InvalidJson,
    }
}

fn parseRow(ctx: Context, filters: model.DateFilters, consumer: provider.EventConsumer, row_value: std.json.Value) !void {
    const obj = switch (row_value) {
        .object => |o| o,
        else => return,
    };

    const thread_id = try getObjectString(ctx.allocator, obj, "id") orelse return;
    defer ctx.allocator.free(thread_id);

    const updated_at = try getObjectString(ctx.allocator, obj, "updated_at") orelse return;
    defer ctx.allocator.free(updated_at);

    const data_type_owned = try getObjectString(ctx.temp_allocator, obj, "data_type");
    const data_type = data_type_owned orelse "zstd";
    defer if (data_type_owned) |dt| ctx.temp_allocator.free(dt);

    const data_hex = try getObjectString(ctx.temp_allocator, obj, "data_hex") orelse return;
    defer ctx.temp_allocator.free(data_hex);

    const blob_len = data_hex.len / 2;
    if (data_hex.len % 2 != 0) {
        log.warn("invalid hex length for thread {s} (len={d})", .{ thread_id, data_hex.len });
        return;
    }
    const blob = try ctx.temp_allocator.alloc(u8, blob_len);
    errdefer ctx.temp_allocator.free(blob);
    _ = std.fmt.hexToBytes(blob, data_hex) catch |err| {
        log.warn("invalid hex data for thread {s} ({s})", .{ thread_id, @errorName(err) });
        return;
    };
    defer ctx.temp_allocator.free(blob);

    const json_data = decompressIfNeeded(ctx.allocator, blob, data_type) catch |err| {
        log.warn("decompress failed ({s})", .{@errorName(err)});
        return;
    };
    defer ctx.allocator.free(json_data);

    parseThread(ctx, filters, consumer, thread_id, updated_at, json_data) catch |err| {
        log.warn("parse thread failed ({s})", .{@errorName(err)});
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
    ctx: Context,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    thread_id: []const u8,
    updated_at: []const u8,
    json_bytes: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.temp_allocator, json_bytes, .{});
    defer parsed.deinit();

    const root = parsed.value;
    const obj = switch (root) {
        .object => |o| o,
        else => return error.InvalidJson,
    };

    var model_name: ?[]const u8 = null;
    defer if (model_name) |m| ctx.allocator.free(m);
    if (obj.get("model")) |model_val| {
        model_name = try parseModelValue(ctx.allocator, model_val);
    }

    if (obj.get("request_token_usage")) |usage_val| {
        try parseRequestUsageValue(ctx, filters, consumer, usage_val, thread_id, updated_at, model_name);
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
    ctx: Context,
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
    const timestamp_info = (try provider.timestampFromSlice(ctx.allocator, updated_at, filters.timezone_offset_minutes)) orelse return;
    defer ctx.allocator.free(timestamp_info.text);

    var it = obj.iterator();
    while (it.next()) |entry| {
        const req_id = entry.key_ptr.*;
        try parseUsageEntryValue(ctx, filters, consumer, req_id, entry.value_ptr.*, thread_id, timestamp_info, model_name);
    }
}

fn parseUsageEntryValue(
    ctx: Context,
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    req_id: []const u8,
    usage_val: std.json.Value,
    thread_id: []const u8,
    timestamp_info: provider.TimestampInfo,
    model_name: ?[]const u8,
) !void {
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
        .display_input_tokens = provider.computeDisplayInput(usage),
    };

    if (consumer.mutex) |m| try m.lock(ctx.io);
    defer if (consumer.mutex) |m| m.unlock(ctx.io);
    try consumer.ingest(consumer.context, ctx.allocator, &event, filters);
}
