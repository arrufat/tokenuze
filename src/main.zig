const std = @import("std");
const builtin = @import("builtin");
const tokenuze = @import("tokenuze");
const cli = @import("cli.zig");
const build_options = @import("build_options");

pub const std_options: std.Options = .{
    .log_level = .debug,
    .logFn = tokenuze.logFn,
};

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
    if (options.machine_id) {
        try printMachineId(allocator);
        return;
    }
    if (options.upload) {
        var uploads = std.ArrayList(tokenuze.uploader.ProviderUpload).empty;
        defer uploads.deinit(allocator);

        for (tokenuze.providers, 0..) |provider, idx| {
            if (!options.providers.includesIndex(idx)) continue;
            var single = tokenuze.ProviderSelection.initEmpty();
            single.includeIndex(idx);
            var report = try tokenuze.collectUploadReport(allocator, options.filters, single);
            const entry = tokenuze.uploader.ProviderUpload{
                .name = provider.name,
                .daily_summary = report.daily_json,
                .sessions_summary = report.sessions_json,
                .weekly_summary = report.weekly_json,
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
            allocator.free(entry.weekly_summary);
        };
        try tokenuze.uploader.run(
            allocator,
            uploads.items,
            options.filters.timezone_offset_minutes,
        );
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
