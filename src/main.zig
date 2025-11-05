const std = @import("std");
const builtin = @import("builtin");
const tokenuze = @import("tokenuze");

const CliError = error{
    InvalidUsage,
};

const CliOptions = struct {
    filters: tokenuze.DateFilters = .{},
    machine_id: bool = false,
    providers: tokenuze.ProviderSelection = tokenuze.ProviderSelection.initAll(),
};

var debug_allocator = std.heap.DebugAllocator(.{}){};

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

    const options = parseOptions(allocator) catch {
        std.process.exit(1);
    };
    if (options.machine_id) {
        try printMachineId(allocator);
        return;
    }
    try tokenuze.run(allocator, options.filters, options.providers);
}

fn parseOptions(allocator: std.mem.Allocator) CliError!CliOptions {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // program name

    var options = CliOptions{};
    var models_specified = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--since")) {
            const value = args.next() orelse return cliError("missing value for --since", .{});
            if (options.filters.since != null) return cliError("--since provided more than once", .{});
            const iso = tokenuze.parseFilterDate(value) catch |err| switch (err) {
                error.InvalidFormat => return cliError("--since expects date in YYYYMMDD", .{}),
                error.InvalidDate => return cliError("--since is not a valid calendar date", .{}),
            };
            options.filters.since = iso;
            continue;
        }

        if (std.mem.eql(u8, arg, "--until")) {
            const value = args.next() orelse return cliError("missing value for --until", .{});
            if (options.filters.until != null) return cliError("--until provided more than once", .{});
            const iso = tokenuze.parseFilterDate(value) catch |err| switch (err) {
                error.InvalidFormat => return cliError("--until expects date in YYYYMMDD", .{}),
                error.InvalidDate => return cliError("--until is not a valid calendar date", .{}),
            };
            options.filters.until = iso;
            continue;
        }

        if (std.mem.eql(u8, arg, "--pretty")) {
            options.filters.pretty_output = true;
            continue;
        }

        if (std.mem.eql(u8, arg, "--machine-id")) {
            if (options.machine_id) return cliError("--machine-id provided more than once", .{});
            options.machine_id = true;
            continue;
        }

        if (std.mem.eql(u8, arg, "--model")) {
            const value = args.next() orelse return cliError("missing value for --model", .{});
            if (!models_specified) {
                models_specified = true;
                options.providers = tokenuze.ProviderSelection.initEmpty();
            }
            if (tokenuze.findProviderIndex(value)) |index| {
                options.providers.includeIndex(index);
            } else {
                return cliError("unknown model '{s}' (expected one of: {s})", .{ value, providerListDescription() });
            }
            continue;
        }

        if (std.mem.startsWith(u8, arg, "-")) {
            return cliError("unknown option: {s}", .{arg});
        }

        return cliError("unexpected argument: {s}", .{arg});
    }

    if (options.machine_id) {
        if (options.filters.since != null or options.filters.until != null or options.filters.pretty_output or models_specified) {
            return cliError("--machine-id cannot be combined with other flags", .{});
        }
    }

    if (options.filters.since) |since_value| {
        if (options.filters.until) |until_value| {
            if (std.mem.lessThan(u8, until_value[0..], since_value[0..])) {
                return cliError("--until must be on or after --since", .{});
            }
        }
    }

    return options;
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

fn cliError(comptime fmt: []const u8, args: anytype) CliError {
    std.debug.print("error: " ++ fmt ++ "\n", args);
    return CliError.InvalidUsage;
}

fn providerListDescription() []const u8 {
    return tokenuze.provider_list_description;
}
