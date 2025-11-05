const std = @import("std");
const builtin = @import("builtin");
const tokenuze = @import("tokenuze");

const CliError = error{
    InvalidUsage,
    OutOfMemory,
};

const CliOptions = struct {
    filters: tokenuze.DateFilters = .{},
    machine_id: bool = false,
    show_help: bool = false,
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

    const options = parseOptions(allocator) catch |err| switch (err) {
        CliError.InvalidUsage => {
            std.process.exit(1);
        },
        else => return err,
    };
    if (options.show_help) {
        try printHelp();
        return;
    }
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
    var agents_specified = false;
    var machine_id_only = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            options.show_help = true;
            break;
        }

        if (std.mem.eql(u8, arg, "--machine-id")) {
            if (!options.machine_id) {
                options.machine_id = true;
                options.filters = .{};
                options.providers = tokenuze.ProviderSelection.initAll();
            }
            machine_id_only = true;
            continue;
        }

        if (machine_id_only) continue;

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

        if (std.mem.eql(u8, arg, "--agent")) {
            const value = args.next() orelse return cliError("missing value for --agent", .{});
            if (!agents_specified) {
                agents_specified = true;
                options.providers = tokenuze.ProviderSelection.initEmpty();
            }
            if (tokenuze.findProviderIndex(value)) |index| {
                options.providers.includeIndex(index);
            } else {
                return cliError("unknown agent '{s}' (expected one of: {s})", .{ value, providerListDescription() });
            }
            continue;
        }

        if (std.mem.startsWith(u8, arg, "-")) {
            return cliError("unknown option: {s}", .{arg});
        }

        return cliError("unexpected argument: {s}", .{arg});
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

fn printHelp() !void {
    var buffer: [1024]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&buffer);
    const writer = &stdout.interface;
    try writer.print(
        \\Tokenuze aggregates model usage logs into daily summaries.
        \\Usage:
        \\  tokenuze [options]
        \\
        \\Options:
        \\
    , .{});

    var agent_desc_buffer: [160]u8 = undefined;
    const agent_desc = try std.fmt.bufPrint(
        &agent_desc_buffer,
        "Restrict collection to selected providers (available: {s})",
        .{providerListDescription()},
    );
    const help_lines = [_]OptionLine{
        .{ .label = "--since YYYYMMDD", .desc = "Only include events on/after the date" },
        .{ .label = "--until YYYYMMDD", .desc = "Only include events on/before the date" },
        .{ .label = "--pretty", .desc = "Expand JSON output for readability" },
        .{ .label = "--agent <name>", .desc = agent_desc },
        .{ .label = "--machine-id", .desc = "Print the stable machine id and exit" },
        .{ .label = "-h, --help", .desc = "Show this message and exit" },
    };

    var max_label: usize = 0;
    for (help_lines) |line| {
        if (line.label.len > max_label) max_label = line.label.len;
    }

    for (help_lines) |line| {
        try printOptionLine(writer, line.label, line.desc, max_label);
    }

    try writer.print(
        \\
        \\When no providers are specified, Tokenuze queries all known providers.
        \\
    , .{});
    try writer.flush();
}

const OptionLine = struct {
    label: []const u8,
    desc: []const u8,
};

fn printOptionLine(writer: anytype, label: []const u8, desc: []const u8, max_label: usize) !void {
    try writer.print("  {s}", .{label});
    var padding = if (max_label > label.len) max_label - label.len else 0;
    while (padding > 0) : (padding -= 1) try writer.writeByte(' ');
    try writer.print("  {s}\n", .{desc});
}

fn cliError(comptime fmt: []const u8, args: anytype) CliError {
    std.debug.print("error: " ++ fmt ++ "\n", args);
    return CliError.InvalidUsage;
}

fn providerListDescription() []const u8 {
    return tokenuze.provider_list_description;
}
