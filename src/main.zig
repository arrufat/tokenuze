const std = @import("std");
const builtin = @import("builtin");
const tokenuze = @import("tokenuze");
const cli = @import("cli.zig");
const build_options = @import("build_options");

pub const std_options: std.Options = tokenuze.std_options;

var debug_allocator: std.heap.DebugAllocator(.{}) = .init;

pub fn main() !void {
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

    const options = cli.parseOptions(allocator) catch |err| switch (err) {
        cli.CliError.InvalidUsage => {
            std.process.exit(1);
        },
        else => return err,
    };
    tokenuze.setLogLevel(options.log_level);
    if (options.show_help) {
        try cli.printHelp();
        return;
    }
    if (options.show_version) {
        try cli.printVersion(build_options.version);
        return;
    }
    if (options.list_agents) {
        try cli.printAgentList(allocator);
        return;
    }
    if (options.machine_id) {
        try printMachineId(allocator);
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
            var report = try tokenuze.collectUploadReportWithCache(allocator, options.filters, single, &pricing_cache);
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
        try tokenuze.uploader.run(
            allocator,
            uploads.items,
            options.filters.timezone_offset_minutes,
        );
        if (!options.output_explicit) {
            return;
        }
    }
    if (options.sessions) {
        try handleSessionsOutput(allocator, options);
        return;
    }

    try tokenuze.run(allocator, options.filters, options.providers);
}

fn printMachineId(allocator: std.mem.Allocator) !void {
    const id = try tokenuze.machine_id.getMachineId(allocator);
    var buffer: [256]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&buffer);
    const writer = &stdout.interface;
    try writer.print("{s}\n", .{id[0..]});
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}

fn handleSessionsOutput(allocator: std.mem.Allocator, options: cli.CliOptions) !void {
    var buffer: [4096]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&buffer);
    const writer = &stdout.interface;

    var cache = tokenuze.PricingCache.init(allocator);
    defer cache.deinit(allocator);

    var recorder = try tokenuze.collectSessionsWithCache(
        allocator,
        options.filters,
        options.providers,
        &cache,
    );
    defer recorder.deinit(allocator);

    if (options.filters.output_format == .table) {
        const tz_offset: i32 = @intCast(options.filters.timezone_offset_minutes);
        try tokenuze.renderSessionsTable(writer, allocator, &recorder, tz_offset);
    } else {
        const json = try recorder.renderJson(allocator, options.filters.pretty_output);
        defer allocator.free(json);
        try writer.print("{s}\n", .{json});
    }
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}
