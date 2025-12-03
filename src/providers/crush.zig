const std = @import("std");
const model = @import("../model.zig");
const provider = @import("provider.zig");
const test_helpers = @import("test_helpers.zig");
const testing = std.testing;

const db_dirname = ".crush";
const db_filename = "crush.db";

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
    var db_paths = try findCrushDbPaths(shared_allocator, temp_allocator);
    defer {
        for (db_paths.items) |p| shared_allocator.free(p);
        db_paths.deinit(shared_allocator);
    }

    if (db_paths.items.len == 0) {
        std.log.info("crush: skipping, no .crush/crush.db found under cwd", .{});
        return;
    }

    const progress_node: ?std.Progress.Node = if (progress) |p|
        p.start("crush_dbs", db_paths.items.len)
    else
        null;

    var work_state = WorkState{
        .paths = db_paths.items,
        .next_index = .init(0),
        .filters = filters,
        .consumer = consumer,
        .shared_allocator = shared_allocator,
        .progress = progress_node,
    };

    const cpu_count = std.Thread.getCpuCount() catch |err| blk: {
        std.log.debug("crush: getCpuCount failed, defaulting to 1 ({s})", .{@errorName(err)});
        break :blk 1;
    };
    const worker_count = @max(@as(usize, 1), @min(db_paths.items.len, cpu_count));

    var threads = try std.ArrayListUnmanaged(std.Thread).initCapacity(shared_allocator, worker_count);
    defer threads.deinit(shared_allocator);

    for (0..worker_count) |_| {
        const t = try std.Thread.spawn(.{}, workerMain, .{&work_state});
        threads.appendAssumeCapacity(t);
    }

    for (threads.items) |t| {
        t.join();
    }
}

pub fn loadPricingData(shared_allocator: std.mem.Allocator, pricing: *model.PricingMap) !void {
    _ = shared_allocator;
    _ = pricing;
}

const WorkState = struct {
    paths: [][]u8,
    next_index: std.atomic.Value(usize),
    filters: model.DateFilters,
    consumer: provider.EventConsumer,
    shared_allocator: std.mem.Allocator,
    progress: ?std.Progress.Node,
};

fn workerMain(state: *WorkState) void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const temp_alloc = arena.allocator();

    while (true) {
        const idx = state.next_index.fetchAdd(1, .acq_rel);
        if (idx >= state.paths.len) break;
        const path = state.paths[idx];
        processDb(state, temp_alloc, path);
        _ = arena.reset(.retain_capacity);
        if (state.progress) |p| p.completeOne();
    }
}

fn processDb(state: *const WorkState, temp_allocator: std.mem.Allocator, db_path: []const u8) void {
    std.fs.cwd().access(db_path, .{}) catch |err| {
        if (err == error.FileNotFound) {
            std.log.debug("crush: skipping missing db at {s}", .{db_path});
            return;
        }
        std.log.warn("crush: access failed for {s} ({s})", .{ db_path, @errorName(err) });
        return;
    };

    const json_rows = runSqliteQuery(temp_allocator, db_path) catch |err| {
        std.log.info("crush: skipping {s}, sqlite3 query failed ({s})", .{ db_path, @errorName(err) });
        return;
    };
    defer temp_allocator.free(json_rows);

    parseRows(state.shared_allocator, temp_allocator, state.filters, state.consumer, json_rows) catch |err| {
        std.log.warn("crush: failed to parse sqlite output from {s} ({s})", .{ db_path, @errorName(err) });
    };
}

fn findCrushDbPaths(allocator: std.mem.Allocator, temp_allocator: std.mem.Allocator) !std.ArrayList([]u8) {
    var list = std.ArrayList([]u8).empty;
    errdefer {
        for (list.items) |p| allocator.free(p);
        list.deinit(allocator);
    }

    var queue: std.ArrayList([]u8) = .empty;
    defer queue.deinit(temp_allocator);
    try queue.append(temp_allocator, try temp_allocator.dupe(u8, "."));

    while (queue.items.len > 0) {
        const dir_path = queue.pop().?;
        defer temp_allocator.free(dir_path);

        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true, .follow_symlinks = false }) catch |err| {
            std.log.debug("crush: skip dir {s} ({s})", .{ dir_path, @errorName(err) });
            continue;
        };
        defer dir.close();
        var it = dir.iterate();
        while (true) {
            const entry = it.next() catch |err| {
                std.log.debug("crush: iterate failed in {s} ({s})", .{ dir_path, @errorName(err) });
                break;
            } orelse break;

            if (entry.kind == .directory) {
                if (std.mem.eql(u8, entry.name, ".") or std.mem.eql(u8, entry.name, "..")) continue;
                const child = std.fs.path.join(temp_allocator, &.{ dir_path, entry.name }) catch |err| {
                    std.log.debug("crush: join failed for {s}/{s} ({s})", .{ dir_path, entry.name, @errorName(err) });
                    continue;
                };
                queue.append(temp_allocator, child) catch |err| {
                    std.log.debug("crush: queue append failed for {s} ({s})", .{ child, @errorName(err) });
                    temp_allocator.free(child);
                    continue;
                };
                continue;
            }

            if (entry.kind != .file) continue;
            if (!std.mem.eql(u8, entry.name, db_filename)) continue;

            const file_path = std.fs.path.join(temp_allocator, &.{ dir_path, entry.name }) catch |err| {
                std.log.debug("crush: join failed for file {s}/{s} ({s})", .{ dir_path, entry.name, @errorName(err) });
                continue;
            };
            defer temp_allocator.free(file_path);

            const parent_dir = std.fs.path.dirname(file_path) orelse continue;
            const basename = std.fs.path.basename(parent_dir);
            if (!std.mem.eql(u8, basename, db_dirname)) continue;

            const dup = try allocator.dupe(u8, file_path);
            std.log.debug("crush: discovered db at {s}", .{file_path});
            try list.append(allocator, dup);
        }
    }
    return list;
}

test "crush parses sqlite output fixture" {
    const allocator = testing.allocator;
    const count = try test_helpers.runFixtureParse(
        allocator,
        "fixtures/crush/sqlite_output.json",
        parseRows,
    );
    try testing.expect(count > 0);
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
