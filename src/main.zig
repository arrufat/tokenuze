const std = @import("std");
const tokenuze = @import("tokenuze");

const CliError = error{
    InvalidUsage,
};

const CliOptions = struct {
    filters: tokenuze.DateFilters = .{},
    machine_id: bool = false,
    providers: tokenuze.ProviderSelection = .{},
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
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
                options.providers = .{ .include_codex = false, .include_gemini = false };
            }
            if (std.mem.eql(u8, value, "codex")) {
                options.providers.include_codex = true;
            } else if (std.mem.eql(u8, value, "gemini")) {
                options.providers.include_gemini = true;
            } else {
                return cliError("unknown model '{s}' (expected 'codex' or 'gemini')", .{value});
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
