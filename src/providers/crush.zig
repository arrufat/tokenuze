const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");

const parse_ctx = provider.ParseContext{
    .provider_name = "crush",
    .legacy_fallback_model = null,
    .cached_counts_overlap_input = false,
};

const db_path_parts = [_][]const u8{ ".crush", "crush.db" };

pub const EventConsumer = struct {
    context: *anyopaque,
    mutex: ?*std.Thread.Mutex = null,
    ingest: *const fn (*anyopaque, std.mem.Allocator, *const model.TokenUsageEvent, model.DateFilters) anyerror!void,
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

    const consumer = EventConsumer{
        .context = @ptrCast(&summary_ctx),
        .mutex = &builder_mutex,
        .ingest = struct {
            fn ingest(ctx_ptr: *anyopaque, allocator: std.mem.Allocator, event: *const model.TokenUsageEvent, f: model.DateFilters) anyerror!void {
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
    consumer: EventConsumer,
    progress: ?std.Progress.Node,
) !void {
    _ = progress;
    const db_path = try resolveDbPath(shared_allocator);
    defer shared_allocator.free(db_path);

    // Check if db exists
    std.fs.cwd().access(db_path, .{}) catch |err| {
        if (err == error.FileNotFound) {
            std.log.info("crush: skipping, .crush/crush.db not found", .{});
            return;
        }
        return err;
    };

    const json_rows = runSqliteQuery(temp_allocator, db_path) catch |err| {
        std.log.info("crush: skipping, sqlite3 query failed ({s})", .{@errorName(err)});
        return;
    };
    defer temp_allocator.free(json_rows);

    parseRows(shared_allocator, temp_allocator, filters, consumer, json_rows) catch |err| {
        std.log.warn("crush: failed to parse sqlite output ({s})", .{@errorName(err)});
    };
}

pub fn loadPricingData(shared_allocator: std.mem.Allocator, pricing: *model.PricingMap) !void {
    _ = shared_allocator;
    _ = pricing;
}

fn resolveDbPath(allocator: std.mem.Allocator) ![]u8 {
    return std.fs.path.join(allocator, &db_path_parts);
}

fn runSqliteQuery(allocator: std.mem.Allocator, db_path: []const u8) ![]u8 {
    // Query to get sessions with usage, joining messages to find the model.
    // We prioritize messages that have a non-empty model.
    const query =
        \\SELECT
        \\  s.id,
        \\  strftime('%Y-%m-%dT%H:%M:%SZ', s.updated_at, 'unixepoch') as timestamp,
        \\  s.prompt_tokens,
        \\  s.completion_tokens,
        \\  (SELECT model FROM messages m WHERE m.session_id = s.id AND m.model != '' LIMIT 1) as model
        \\FROM sessions s
        \\WHERE s.prompt_tokens > 0 OR s.completion_tokens > 0
    ;
    var argv = [_][]const u8{ "sqlite3", "-json", db_path, query };

    var result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &argv,
        .max_output_bytes = 64 * 1024 * 1024,
    }) catch |err| {
        if (err == error.FileNotFound) {
            std.log.err("crush: sqlite3 CLI not found; install sqlite3 to enable Crush ingestion", .{});
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
            std.log.err("crush: sqlite3 CLI not found; install sqlite3 to enable Crush ingestion", .{});
        } else {
            std.log.warn("crush: sqlite3 exited with code {d}: {s}", .{ exit_code, result.stderr });
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
    consumer: EventConsumer,
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
    consumer: EventConsumer,
    row_value: std.json.Value,
) !void {
    const obj = switch (row_value) {
        .object => |o| o,
        else => return,
    };

    const session_id = try getObjectString(shared_allocator, obj, "id") orelse return;
    defer shared_allocator.free(session_id);

    const timestamp_str = try getObjectString(temp_allocator, obj, "timestamp") orelse return;
    defer temp_allocator.free(timestamp_str);

    const model_name_owned = try getObjectString(shared_allocator, obj, "model");
    const model_name = model_name_owned orelse "unknown";
    defer if (model_name_owned) |m| shared_allocator.free(m);

    const prompt_tokens = getObjectU64(obj, "prompt_tokens");
    const completion_tokens = getObjectU64(obj, "completion_tokens");

    if (prompt_tokens == 0 and completion_tokens == 0) return;

    const timestamp_info = (try provider.timestampFromSlice(shared_allocator, timestamp_str, filters.timezone_offset_minutes)) orelse return;
    defer shared_allocator.free(timestamp_info.text);

    const usage = model.TokenUsage{
        .input_tokens = prompt_tokens,
        .output_tokens = completion_tokens,
        .total_tokens = prompt_tokens + completion_tokens,
    };

    const event = model.TokenUsageEvent{
        .session_id = session_id,
        .timestamp = timestamp_info.text,
        .local_iso_date = timestamp_info.local_iso_date,
        .model = model_name,
        .usage = usage,
        .is_fallback = false, // Crush doesn't seem to have fallback indicator in DB
        .display_input_tokens = prompt_tokens,
    };

    if (consumer.mutex) |m| m.lock();
    defer if (consumer.mutex) |m| m.unlock();
    try consumer.ingest(consumer.context, shared_allocator, &event, filters);
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

fn getObjectU64(obj: std.json.ObjectMap, key: []const u8) u64 {
    if (obj.get(key)) |val| {
        return switch (val) {
            .integer => |v| if (v >= 0) @as(u64, @intCast(v)) else 0,
            .float => |v| if (v >= 0) @as(u64, @intFromFloat(v)) else 0,
            else => 0,
        };
    }
    return 0;
}
