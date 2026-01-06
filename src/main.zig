const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

const tokenuze = @import("tokenuze");
pub const std_options: std.Options = tokenuze.std_options;

const cli = @import("cli.zig");

var debug_allocator: std.heap.DebugAllocator(.{}) = .init;

pub fn main(init: std.process.Init) !void {
    const native_os = builtin.target.os.tag;
    const choice = blk: {
        if (native_os == .wasi) break :blk .{ .allocator = std.heap.wasm_allocator, .is_debug = false };
        break :blk switch (builtin.mode) {
            .Debug, .ReleaseSafe => .{ .allocator = debug_allocator.allocator(), .is_debug = true },
            .ReleaseFast, .ReleaseSmall => .{ .allocator = std.heap.smp_allocator, .is_debug = false },
        };
    };
    defer if (choice.is_debug) {
        _ = debug_allocator.deinit();
    };

    const allocator = choice.allocator;
    const ctx = tokenuze.Context{
        .allocator = allocator,
        .temp_allocator = std.heap.page_allocator,
        .io = init.io,
        .environ_map = init.environ_map,
    };

    const options = cli.parseOptions(allocator, init.minimal.args) catch |err| switch (err) {
        cli.CliError.InvalidUsage => {
            std.process.exit(1);
        },
        else => return err,
    };

    tokenuze.setLogLevel(options.log_level);
    if (options.show_help) {
        try cli.printHelp(ctx.io);
        return;
    }
    if (options.show_version) {
        try cli.printVersion(ctx.io, build_options.version);
        return;
    }
    if (options.list_agents) {
        try cli.printAgentList(ctx);
        return;
    }
    if (options.machine_id) {
        try printMachineId(ctx);
        return;
    }
    if (options.upload) {
        var uploads = std.ArrayList(tokenuze.uploader.ProviderUpload).empty;
        defer uploads.deinit(allocator);

        var pricing_cache = tokenuze.PricingCache.init(allocator);
        defer pricing_cache.deinit(allocator);
        try pricing_cache.ensureLoaded(allocator, std.heap.page_allocator, options.providers, null);

        for (tokenuze.providers, 0..) |provider, idx| {
            if (!options.providers.includesIndex(idx)) continue;
            var single = tokenuze.ProviderSelection.initEmpty();
            single.includeIndex(idx);
            var report = try tokenuze.collectUploadReportWithCache(ctx, options.filters, single, &pricing_cache);
            const entry = tokenuze.uploader.ProviderUpload{
                .name = provider.name,
                .daily_summary = report.daily_json,
                .sessions_summary = report.sessions_json,
            };
            uploads.append(allocator, entry) catch |err| {
                report.deinit(allocator);
                return err;
            };
        }

        if (uploads.items.len == 0) {
            std.log.err("No providers selected for upload; use --agent to pick at least one provider.", .{});
            return;
        }

        defer for (uploads.items) |entry| {
            allocator.free(entry.daily_summary);
            allocator.free(entry.sessions_summary);
        };
        try tokenuze.uploader.run(ctx, uploads.items, options.filters.timezone_offset_minutes);
        if (!options.output_explicit) {
            return;
        }
    }
    if (options.sessions) {
        try handleSessionsOutput(ctx, options);
        return;
    }

    try tokenuze.runWithContext(ctx, options.filters, options.providers);
}

fn printMachineId(ctx: tokenuze.Context) !void {
    const id = try tokenuze.machine_id.getMachineId(ctx);
    var buffer: [256]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(ctx.io, buffer[0..]);
    const writer = &stdout.interface;
    try writer.print("{s}\n", .{id[0..]});
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}

fn handleSessionsOutput(ctx: tokenuze.Context, options: cli.CliOptions) !void {
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(ctx.io, buffer[0..]);
    const writer = &stdout.interface;

    var cache = tokenuze.PricingCache.init(ctx.allocator);
    defer cache.deinit(ctx.allocator);

    var recorder = try tokenuze.collectSessionsWithCacheWithContext(ctx, options.filters, options.providers, &cache);
    defer recorder.deinit(ctx.allocator);

    if (options.filters.output_format == .table) {
        const tz_offset: i32 = @intCast(options.filters.timezone_offset_minutes);
        try tokenuze.renderSessionsTable(writer, ctx.allocator, &recorder, tz_offset);
    } else {
        const json = try recorder.renderJson(ctx.allocator, options.filters.pretty_output);
        defer ctx.allocator.free(json);
        try writer.print("{s}\n", .{json});
    }
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}
