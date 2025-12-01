const std = @import("std");
const builtin = @import("builtin");
const tokenuze = @import("tokenuze");
const testing = std.testing;

pub const CliError = error{
    InvalidUsage,
    OutOfMemory,
};

pub const CliOptions = struct {
    filters: tokenuze.DateFilters = .{},
    machine_id: bool = false,
    show_help: bool = false,
    show_version: bool = false,
    providers: tokenuze.ProviderSelection = tokenuze.ProviderSelection.initAll(),
    upload: bool = false,
    output_explicit: bool = false,
    log_level: std.log.Level = if (builtin.mode == .Debug) .debug else .err,
    sessions: bool = false,
};

const OptionId = enum {
    since,
    until,
    tz,
    pretty,
    table,
    json,
    sessions,
    log_level,
    agent,
    upload,
    machine_id,
    version,
    help,
};

const OptionArgKind = enum {
    flag,
    value,
};

const OptionSpec = struct {
    id: OptionId,
    long_name: []const u8,
    short_name: ?u8 = null,
    value_name: ?[]const u8 = null,
    desc: []const u8,
    kind: OptionArgKind = .flag,
};

const option_specs = [_]OptionSpec{
    .{ .id = .since, .long_name = "since", .value_name = "YYYYMMDD", .desc = "Only include events on/after the date", .kind = .value },
    .{ .id = .until, .long_name = "until", .value_name = "YYYYMMDD", .desc = "Only include events on/before the date", .kind = .value },
    .{ .id = .tz, .long_name = "tz", .value_name = "<offset>", .desc = "Bucket dates in the provided timezone (default: {s})", .kind = .value },
    .{ .id = .table, .long_name = "table", .desc = "Render usage as a table (default behavior)" },
    .{ .id = .json, .long_name = "json", .desc = "Render usage as JSON instead of the table" },
    .{ .id = .pretty, .long_name = "pretty", .desc = "Expand JSON output for readability" },
    .{ .id = .sessions, .long_name = "sessions", .desc = "Render session-level output (table or JSON with --json)" },
    .{ .id = .agent, .long_name = "agent", .value_name = "<name>", .desc = "Restrict collection to selected providers (available: {s})", .kind = .value },
    .{ .id = .log_level, .long_name = "log-level", .value_name = "LEVEL", .desc = "Control logging verbosity (error|warn|info|debug)", .kind = .value },
    .{ .id = .upload, .long_name = "upload", .desc = "Upload Tokenuze JSON via DASHBOARD_API_KEY and DASHBOARD_API_URL envs" },
    .{ .id = .machine_id, .long_name = "machine-id", .desc = "Print the machine id and exit" },
    .{ .id = .version, .long_name = "version", .desc = "Print version number and exit" },
    .{ .id = .help, .long_name = "help", .short_name = 'h', .desc = "Show this message and exit" },
};

threadlocal var provider_desc_buffer: [256]u8 = undefined;

pub fn parseOptions(allocator: std.mem.Allocator) CliError!CliOptions {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.skip(); // program name

    return parseOptionsIterator(&args);
}

fn parseOptionsIterator(args: anytype) CliError!CliOptions {
    var options = CliOptions{};
    var timezone_specified = false;
    var agents_specified = false;
    var machine_id_only = false;
    while (args.next()) |arg| {
        const maybe_spec = try classifyArg(arg);

        if (maybe_spec) |spec| {
            switch (spec.id) {
                .help => {
                    options.show_help = true;
                    break;
                },
                .version => {
                    options.show_version = true;
                    break;
                },
                .machine_id => {
                    if (!options.machine_id) {
                        options.machine_id = true;
                        options.filters = .{};
                        options.providers = tokenuze.ProviderSelection.initAll();
                    }
                    machine_id_only = true;
                    continue;
                },
                else => {},
            }

            if (machine_id_only) {
                if (optionTakesValue(spec)) skipOptionValue(args);
                continue;
            }

            try applyOption(spec, args, &options, &timezone_specified, &agents_specified);
            continue;
        }

        if (machine_id_only) continue;

        return cliError("unexpected argument: {s}", .{arg});
    }

    if (options.filters.since) |since_value| {
        if (options.filters.until) |until_value| {
            if (std.mem.lessThan(u8, until_value[0..], since_value[0..])) {
                return cliError("--until must be on or after --since", .{});
            }
        }
    }

    if (!timezone_specified) {
        const fallback_offset = tokenuze.default_timezone_offset_minutes;
        const detected = tokenuze.detectLocalTimezoneOffsetMinutes() catch fallback_offset;
        const clamped = std.math.clamp(detected, -12 * 60, 14 * 60);
        options.filters.timezone_offset_minutes = @intCast(clamped);
    }

    return options;
}

pub fn printHelp() !void {
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

    const default_tz_offset = tokenuze.detectLocalTimezoneOffsetMinutes() catch tokenuze.default_timezone_offset_minutes;
    var tz_label_buf: [16]u8 = undefined;
    const tz_label = tokenuze.formatTimezoneLabel(&tz_label_buf, default_tz_offset);

    var max_label: usize = 0;
    for (optionSpecs()) |spec| {
        const length = optionLabelLength(&spec);
        if (length > max_label) max_label = length;
    }

    for (optionSpecs()) |spec| {
        var desc_buf: [256]u8 = undefined;
        const desc = optionDescription(&spec, tz_label, desc_buf[0..]);
        try printOptionLine(writer, &spec, desc, max_label);
    }

    try writer.print(
        \\
        \\When no providers are specified, Tokenuze queries all known providers.
        \\
    , .{});
    try writer.flush();
}

pub fn printVersion(version: []const u8) !void {
    var buffer: [256]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&buffer);
    const writer = &stdout.interface;
    try writer.print("{s}\n", .{version});
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}

fn optionSpecs() []const OptionSpec {
    return option_specs[0..];
}

fn classifyArg(arg: []const u8) CliError!?*const OptionSpec {
    if (!std.mem.startsWith(u8, arg, "-")) return null;
    if (arg.len < 2) return cliError("unknown option: {s}", .{arg});

    if (arg[1] == '-') {
        if (arg.len == 2) return cliError("unknown option: {s}", .{arg});
        const name = arg[2..];
        if (findLongOption(name)) |spec| return spec;
        return cliError("unknown option: {s}", .{arg});
    }

    if (arg.len != 2) return cliError("unknown option: {s}", .{arg});
    if (findShortOption(arg[1])) |spec| return spec;
    return cliError("unknown option: {s}", .{arg});
}

fn findLongOption(name: []const u8) ?*const OptionSpec {
    for (&option_specs) |*spec| {
        if (std.mem.eql(u8, spec.long_name, name)) return spec;
    }
    return null;
}

fn findShortOption(short: u8) ?*const OptionSpec {
    for (&option_specs) |*spec| {
        if (spec.short_name) |alias| {
            if (alias == short) return spec;
        }
    }
    return null;
}

fn optionTakesValue(spec: *const OptionSpec) bool {
    return spec.kind == .value;
}

fn skipOptionValue(args: anytype) void {
    _ = args.next();
}

fn applyOption(
    spec: *const OptionSpec,
    args: anytype,
    options: *CliOptions,
    timezone_specified: *bool,
    agents_specified: *bool,
) CliError!void {
    switch (spec.id) {
        .upload => options.upload = true,
        .pretty => options.filters.pretty_output = true,
        .table => {
            options.filters.output_format = .table;
            options.output_explicit = true;
        },
        .json => {
            options.filters.output_format = .json;
            options.output_explicit = true;
        },
        .sessions => {
            options.sessions = true;
            options.output_explicit = true;
        },
        .log_level => {
            const value = args.next() orelse return missingValueError(spec.long_name);
            options.log_level = try parseLogLevelArg(value);
        },
        .since => {
            const value = args.next() orelse return missingValueError(spec.long_name);
            if (options.filters.since != null) return cliError("--since provided more than once", .{});
            const iso = tokenuze.parseFilterDate(value) catch |err| switch (err) {
                error.InvalidFormat => return cliError("--since expects date in YYYYMMDD", .{}),
                error.InvalidDate => return cliError("--since is not a valid calendar date", .{}),
            };
            options.filters.since = iso;
        },
        .until => {
            const value = args.next() orelse return missingValueError(spec.long_name);
            if (options.filters.until != null) return cliError("--until provided more than once", .{});
            const iso = tokenuze.parseFilterDate(value) catch |err| switch (err) {
                error.InvalidFormat => return cliError("--until expects date in YYYYMMDD", .{}),
                error.InvalidDate => return cliError("--until is not a valid calendar date", .{}),
            };
            options.filters.until = iso;
        },
        .tz => {
            const value = args.next() orelse return missingValueError(spec.long_name);
            const offset = tokenuze.parseTimezoneOffsetMinutes(value) catch {
                return cliError("--tz expects an offset like '+09', '-05:30', or 'UTC'", .{});
            };
            options.filters.timezone_offset_minutes = @intCast(offset);
            timezone_specified.* = true;
        },
        .agent => {
            const value = args.next() orelse return missingValueError(spec.long_name);
            if (!agents_specified.*) {
                agents_specified.* = true;
                options.providers = tokenuze.ProviderSelection.initEmpty();
            }
            if (tokenuze.findProviderIndex(value)) |index| {
                options.providers.includeIndex(index);
            } else {
                return cliError("unknown agent '{s}' (expected one of: {s})", .{ value, providerListDescription() });
            }
        },
        else => unreachable,
    }
}

fn missingValueError(name: []const u8) CliError {
    return cliError("missing value for --{s}", .{name});
}

fn optionDescription(spec: *const OptionSpec, tz_label: []const u8, buffer: []u8) []const u8 {
    return switch (spec.id) {
        .agent => std.fmt.bufPrint(
            buffer,
            "Restrict collection to selected providers (available: {s})",
            .{providerListDescription()},
        ) catch spec.desc,
        .tz => std.fmt.bufPrint(
            buffer,
            "Bucket dates in the provided timezone (default: {s})",
            .{tz_label},
        ) catch spec.desc,
        .log_level => std.fmt.bufPrint(
            buffer,
            "Control logging verbosity (error|warn|info|debug, default: error)",
            .{},
        ) catch spec.desc,
        else => spec.desc,
    };
}

fn printOptionLine(writer: anytype, spec: *const OptionSpec, desc: []const u8, max_label: usize) !void {
    try writer.writeAll("  ");
    try writeOptionLabel(writer, spec);
    const label_len = optionLabelLength(spec);
    var padding = if (max_label > label_len) max_label - label_len else 0;
    while (padding > 0) : (padding -= 1) try writer.writeByte(' ');
    try writer.print("  {s}\n", .{desc});
}

fn writeOptionLabel(writer: anytype, spec: *const OptionSpec) !void {
    if (spec.short_name) |short| {
        try writer.print("-{c}", .{short});
        if (spec.long_name.len != 0) {
            try writer.writeAll(", ");
        }
    }
    if (spec.long_name.len != 0) {
        try writer.print("--{s}", .{spec.long_name});
    }
    if (spec.value_name) |value| {
        try writer.print(" {s}", .{value});
    }
}

fn optionLabelLength(spec: *const OptionSpec) usize {
    var length: usize = 0;
    if (spec.short_name != null) {
        length += 2; // "-x"
        if (spec.long_name.len != 0) {
            length += 2; // ", "
        }
    }
    if (spec.long_name.len != 0) {
        length += 2 + spec.long_name.len; // "--"
    }
    if (spec.value_name) |value| {
        length += 1 + value.len;
    }
    return length;
}

fn cliError(comptime fmt: []const u8, args: anytype) CliError {
    if (!builtin.is_test) {
        std.debug.print("error: " ++ fmt ++ "\n", args);
    }
    return CliError.InvalidUsage;
}

fn providerListDescription() []const u8 {
    return tokenuze.providerListDescription(&provider_desc_buffer);
}

fn parseLogLevelArg(value: []const u8) CliError!std.log.Level {
    const mapping = [_]struct {
        name: []const u8,
        level: std.log.Level,
    }{
        .{ .name = "error", .level = .err },
        .{ .name = "err", .level = .err },
        .{ .name = "warn", .level = .warn },
        .{ .name = "warning", .level = .warn },
        .{ .name = "info", .level = .info },
        .{ .name = "debug", .level = .debug },
    };

    inline for (mapping) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.name, value)) return entry.level;
    }

    return cliError(
        "invalid log level '{s}' (expected one of: error, warn, info, debug)",
        .{value},
    );
}

const TestIterator = struct {
    items: []const []const u8,
    index: usize = 0,

    fn init(items: []const []const u8) TestIterator {
        return .{ .items = items, .index = 0 };
    }

    fn next(self: *TestIterator) ?[]const u8 {
        if (self.index >= self.items.len) return null;
        const value = self.items[self.index];
        self.index += 1;
        return value;
    }
};

test "cli parses defaults with no args" {
    var iter = TestIterator.init(&.{});
    const options = try parseOptionsIterator(&iter);
    try testing.expect(!options.show_help);
    try testing.expect(!options.show_version);
    try testing.expect(!options.filters.pretty_output);
    try testing.expect(!options.machine_id);
    try testing.expect(options.filters.output_format == .table);
    try testing.expect(!options.output_explicit);
    try testing.expectEqual(
        if (builtin.mode == .Debug) std.log.Level.debug else std.log.Level.err,
        options.log_level,
    );
}

test "cli parses filters and agent selection" {
    const args = &.{
        "--since",  "20250101",
        "--until",  "20250131",
        "--tz",     "+02",
        "--pretty", "--table",
        "--agent",  "codex",
    };
    var iter = TestIterator.init(args);
    const options = try parseOptionsIterator(&iter);
    const expected_since = "2025-01-01".*;
    const expected_until = "2025-01-31".*;
    try testing.expectEqual(expected_since, options.filters.since.?);
    try testing.expectEqual(expected_until, options.filters.until.?);
    try testing.expect(options.filters.pretty_output);
    try testing.expect(options.filters.output_format == .table);
    try testing.expect(options.output_explicit);
    try testing.expectEqual(@as(i16, 2 * 60), options.filters.timezone_offset_minutes);
    const codex_index = tokenuze.findProviderIndex("codex") orelse unreachable;
    try testing.expect(options.providers.includesIndex(codex_index));
}

test "cli parses --json" {
    var iter = TestIterator.init(&.{"--json"});
    const options = try parseOptionsIterator(&iter);
    try testing.expect(options.filters.output_format == .json);
    try testing.expect(options.output_explicit);
}

test "cli parses --sessions" {
    var iter = TestIterator.init(&.{"--sessions"});
    const options = try parseOptionsIterator(&iter);
    try testing.expect(options.sessions);
    try testing.expect(options.filters.output_format == .table);
    try testing.expect(options.output_explicit);
}

test "cli --json overrides --table" {
    var iter = TestIterator.init(&.{ "--table", "--json" });
    const options = try parseOptionsIterator(&iter);
    try testing.expect(options.filters.output_format == .json);
    try testing.expect(options.output_explicit);
}

test "cli --table overrides --json" {
    var iter = TestIterator.init(&.{ "--json", "--table" });
    const options = try parseOptionsIterator(&iter);
    try testing.expect(options.filters.output_format == .table);
    try testing.expect(options.output_explicit);
}

test "cli parses --log-level" {
    var iter = TestIterator.init(&.{ "--log-level", "debug" });
    const options = try parseOptionsIterator(&iter);
    try testing.expectEqual(std.log.Level.debug, options.log_level);
}

test "cli rejects invalid --log-level" {
    var iter = TestIterator.init(&.{ "--log-level", "chatty" });
    try testing.expectError(CliError.InvalidUsage, parseOptionsIterator(&iter));
}

test "cli rejects duplicate --since" {
    var iter = TestIterator.init(&.{ "--since", "20250101", "--since", "20250102" });
    try testing.expectError(CliError.InvalidUsage, parseOptionsIterator(&iter));
}
