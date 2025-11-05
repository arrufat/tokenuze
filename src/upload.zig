const std = @import("std");
const builtin = @import("builtin");
const Model = @import("model.zig");
const machine_id = @import("machine_id.zig");
const codex = @import("providers/codex.zig");

const DEFAULT_API_URL = "http://localhost:8000";
const UploadError = std.process.Child.RunError || error{CommandFailed};
const ArrayWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    fn init(list: *std.ArrayList(u8), allocator: std.mem.Allocator) ArrayWriter {
        return .{
            .base = .{
                .vtable = &.{
                    .drain = ArrayWriter.drain,
                    .sendFile = std.Io.Writer.unimplementedSendFile,
                    .flush = ArrayWriter.flush,
                    .rebase = std.Io.Writer.defaultRebase,
                },
                .buffer = &.{},
            },
            .list = list,
            .allocator = allocator,
        };
    }

    fn drain(
        writer: *std.Io.Writer,
        data: []const []const u8,
        splat: usize,
    ) std.Io.Writer.Error!usize {
        const self: *ArrayWriter = @fieldParentPtr("base", writer);
        var written: usize = 0;
        for (data) |chunk| {
            if (chunk.len == 0) continue;
            self.list.appendSlice(self.allocator, chunk) catch return error.WriteFailed;
            written += chunk.len;
        }
        if (splat > 1 and data.len != 0) {
            const last = data[data.len - 1];
            const extra = splat - 1;
            for (0..extra) |_| {
                self.list.appendSlice(self.allocator, last) catch return error.WriteFailed;
                written += last.len;
            }
        }
        return written;
    }

    fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
        return;
    }
};

pub fn run(allocator: std.mem.Allocator, filters: Model.DateFilters) !void {
    var env = try EnvConfig.load(allocator);
    defer env.deinit(allocator);

    if (env.api_key.len == 0) reportMissingApiKey();

    var effective_filters = filters;
    var since_storage: [10]u8 = undefined;
    if (effective_filters.since == null) {
        since_storage = try defaultSinceIso();
        effective_filters.since = since_storage;
    }

    const timestamp = try currentUtcTimestamp(allocator);
    defer allocator.free(timestamp);

    var machine = try machine_id.getMachineId(allocator);
    const hostname = try resolveHostname(allocator);
    defer allocator.free(hostname);
    const username = try resolveUsername(allocator);
    defer allocator.free(username);
    const display_name = try std.fmt.allocPrint(allocator, "{s}@{s}", .{ username, hostname });
    defer allocator.free(display_name);

    var summary_builder = Model.SummaryBuilder.init(allocator);
    defer summary_builder.deinit(allocator);
    var session_acc = SessionAccumulator.init(allocator);
    defer session_acc.deinit();

    var consumer_ctx = UploadIngestContext{
        .builder = &summary_builder,
        .sessions = &session_acc,
    };
    var consumer_mutex = std.Thread.Mutex{};
    const consumer = codex.EventConsumer{
        .context = @ptrCast(&consumer_ctx),
        .mutex = &consumer_mutex,
        .ingest = uploadIngest,
    };

    std.log.info("collecting codex usage for upload...", .{});
    try codex.streamEvents(allocator, std.heap.page_allocator, effective_filters, consumer);

    var pricing_map = Model.PricingMap.init(allocator);
    defer pricing_map.deinit();
    try codex.loadPricingData(allocator, std.heap.page_allocator, &pricing_map);

    try applyPricingAndSort(allocator, &summary_builder, &pricing_map);

    const summaries = summary_builder.items();
    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();
    var totals = Model.SummaryTotals.init();
    defer totals.deinit(allocator);
    Model.accumulateTotals(allocator, summaries, &totals);
    Model.collectMissingModels(allocator, &missing_set, &totals.missing_pricing) catch {};

    var week_ranges = try WeekRanges.compute(allocator, summaries);
    defer week_ranges.deinit();

    const codex_view = CodexReportView{
        .allocator = allocator,
        .sessions = &session_acc,
        .summaries = summaries,
        .week_ranges = week_ranges.items,
        .totals = &totals,
        .pricing = &pricing_map,
    };

    std.log.info("building upload payload...", .{});
    const payload = try buildPayload(
        allocator,
        timestamp,
        machine[0..],
        hostname,
        display_name,
        codex_view,
    );
    defer allocator.free(payload);

    const endpoint = try buildEndpoint(allocator, env.api_url);
    defer allocator.free(endpoint);

    std.log.info("uploading codex usage...", .{});
    var response = sendPayload(allocator, endpoint, env.api_key, @constCast(payload)) catch |err| {
        std.debug.print("⚠️  Connection failed. Is the server running at {s}?\n", .{env.api_url});
        return err;
    };
    defer response.deinit(allocator);

    handleResponse(response);
}

const EnvConfig = struct {
    api_url: []const u8,
    api_key: []const u8,

    fn load(allocator: std.mem.Allocator) !EnvConfig {
        const raw_url = std.process.getEnvVarOwned(allocator, "DASHBOARD_API_URL") catch |err| switch (err) {
            error.EnvironmentVariableNotFound => try allocator.dupe(u8, DEFAULT_API_URL),
            else => return err,
        };
        const trimmed_url = std.mem.trim(u8, raw_url, " \n\r\t");
        const api_url = try allocator.dupe(u8, trimmed_url);
        allocator.free(raw_url);

        const raw_key = std.process.getEnvVarOwned(allocator, "DASHBOARD_API_KEY") catch |err| switch (err) {
            error.EnvironmentVariableNotFound => try allocator.dupe(u8, ""),
            else => return err,
        };
        const trimmed_key = std.mem.trim(u8, raw_key, " \n\r\t");
        const api_key = try allocator.dupe(u8, trimmed_key);
        allocator.free(raw_key);

        return .{ .api_url = api_url, .api_key = api_key };
    }

    fn deinit(self: *EnvConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.api_url);
        allocator.free(self.api_key);
        self.* = undefined;
    }
};

fn reportMissingApiKey() noreturn {
    std.debug.print("❌ Error: DASHBOARD_API_KEY not set\n", .{});
    std.debug.print("   Please add to ~/.zshrc or ~/.bashrc:\n", .{});
    std.debug.print("   export DASHBOARD_API_KEY=\"sk_team_your_api_key_here\"\n\n", .{});
    std.debug.print("   For production, also set:\n", .{});
    std.debug.print("   export DASHBOARD_API_URL=\"https://your-dashboard.com\"\n\n", .{});
    std.process.exit(1);
}

fn currentUtcTimestamp(allocator: std.mem.Allocator) ![]u8 {
    return runStringCommand(allocator, &.{ "date", "-u", "+%Y-%m-%dT%H:%M:%SZ" });
}

fn defaultSinceIso() ![10]u8 {
    const compact = try determineSinceDate();
    defer std.heap.page_allocator.free(compact);
    if (compact.len != 8) return error.CommandFailed;
    var iso: [10]u8 = undefined;
    iso[0..4].* = compact[0..4].*;
    iso[4] = '-';
    iso[5..7].* = compact[4..6].*;
    iso[7] = '-';
    iso[8..10].* = compact[6..8].*;
    return iso;
}

fn determineSinceDate() UploadError![]u8 {
    if (builtin.os.tag == .macos) {
        return runStringCommand(std.heap.page_allocator, &.{ "date", "-v-30d", "+%Y%m%d" });
    }
    return runStringCommand(std.heap.page_allocator, &.{ "date", "-d", "30 days ago", "+%Y%m%d" });
}

fn runStringCommand(allocator: std.mem.Allocator, argv: []const []const u8) UploadError![]u8 {
    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .max_output_bytes = 10 * 1024,
    });
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);
    if (!commandSucceeded(result.term)) return error.CommandFailed;
    const trimmed = std.mem.trim(u8, result.stdout, " \n\r\t");
    return allocator.dupe(u8, trimmed);
}

fn resolveHostname(allocator: std.mem.Allocator) ![]u8 {
    if (std.process.getEnvVarOwned(allocator, "HOSTNAME")) |value| {
        return value;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    if (std.process.getEnvVarOwned(allocator, "COMPUTERNAME")) |value| {
        return value;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    return runStringCommand(allocator, &.{"hostname"});
}

fn resolveUsername(allocator: std.mem.Allocator) ![]u8 {
    if (std.process.getEnvVarOwned(allocator, "USER")) |value| {
        return value;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    if (std.process.getEnvVarOwned(allocator, "USERNAME")) |value| {
        return value;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    return allocator.dupe(u8, "unknown");
}

const ModelUsageStats = struct {
    usage: Model.TokenUsage = .{},
    is_fallback: bool = false,
};

const SessionStats = struct {
    session_id: []const u8,
    last_activity: []const u8 = "",
    usage: Model.TokenUsage = .{},
    models: std.StringHashMap(ModelUsageStats),

    fn init(allocator: std.mem.Allocator, session_id: []const u8) SessionStats {
        return .{
            .session_id = session_id,
            .last_activity = "",
            .usage = .{},
            .models = std.StringHashMap(ModelUsageStats).init(allocator),
        };
    }

    fn deinit(self: *SessionStats, allocator: std.mem.Allocator) void {
        if (self.last_activity.len != 0) allocator.free(self.last_activity);
        var it = self.models.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.models.deinit();
    }

    fn updateTimestamp(self: *SessionStats, allocator: std.mem.Allocator, timestamp: []const u8) !void {
        if (timestamp.len == 0) return;
        if (self.last_activity.len == 0 or std.mem.order(u8, timestamp, self.last_activity) == .gt) {
            if (self.last_activity.len != 0) allocator.free(self.last_activity);
            self.last_activity = try allocator.dupe(u8, timestamp);
        }
    }

    fn addModelUsage(
        self: *SessionStats,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        usage: Model.TokenUsage,
        is_fallback: bool,
    ) !void {
        var gop = try self.models.getOrPut(model_name);
        if (!gop.found_existing) {
            const name_copy = try allocator.dupe(u8, model_name);
            gop.key_ptr.* = name_copy;
            gop.value_ptr.* = .{ .usage = usage, .is_fallback = is_fallback };
        } else {
            gop.value_ptr.usage.add(usage);
            if (is_fallback) gop.value_ptr.is_fallback = true;
        }
        self.usage.add(usage);
    }
};

const SessionAccumulator = struct {
    allocator: std.mem.Allocator,
    sessions: std.StringHashMap(SessionStats),
    totals: Model.TokenUsage = .{},

    fn init(allocator: std.mem.Allocator) SessionAccumulator {
        return .{ .allocator = allocator, .sessions = std.StringHashMap(SessionStats).init(allocator), .totals = .{} };
    }

    fn deinit(self: *SessionAccumulator) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.sessions.deinit();
    }

    fn ingest(self: *SessionAccumulator, event: *Model.TokenUsageEvent, filters: Model.DateFilters) !void {
        if (!Model.dateWithinFilters(filters, event.local_iso_date[0..])) return;
        self.totals.add(event.usage);
        var gop = try self.sessions.getOrPut(event.session_id);
        if (!gop.found_existing) {
            const key_copy = try self.allocator.dupe(u8, event.session_id);
            gop.key_ptr.* = key_copy;
            gop.value_ptr.* = SessionStats.init(self.allocator, key_copy);
        }
        var stats = gop.value_ptr.*;
        try stats.updateTimestamp(self.allocator, event.timestamp);
        try stats.addModelUsage(self.allocator, event.model, event.usage, event.is_fallback);
    }
};

const UploadIngestContext = struct {
    builder: *Model.SummaryBuilder,
    sessions: *SessionAccumulator,
};

fn uploadIngest(
    ctx_ptr: *anyopaque,
    allocator: std.mem.Allocator,
    event: *Model.TokenUsageEvent,
    filters: Model.DateFilters,
) anyerror!void {
    const ctx = @as(*UploadIngestContext, @ptrCast(@alignCast(ctx_ptr)));
    try ctx.builder.ingest(allocator, event, filters);
    try ctx.sessions.ingest(event, filters);
}

fn applyPricingAndSort(
    allocator: std.mem.Allocator,
    builder: *Model.SummaryBuilder,
    pricing: *Model.PricingMap,
) !void {
    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();
    const summaries = builder.items();
    for (summaries) |*summary| {
        Model.applyPricing(allocator, summary, pricing, &missing_set);
        std.sort.pdq(Model.ModelSummary, summary.models.items, {}, modelLessThan);
    }
    std.sort.pdq(Model.DailySummary, summaries, {}, summaryLessThan);
}

fn summaryLessThan(_: void, lhs: Model.DailySummary, rhs: Model.DailySummary) bool {
    return std.mem.lessThan(u8, lhs.iso_date, rhs.iso_date);
}

fn modelLessThan(_: void, lhs: Model.ModelSummary, rhs: Model.ModelSummary) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

const WeekRange = struct {
    iso_start: []const u8,
    display_start: []const u8,
    start_index: usize,
    end_index: usize,
};

const WeekRanges = struct {
    allocator: std.mem.Allocator,
    items: []WeekRange,

    fn compute(allocator: std.mem.Allocator, summaries: []const Model.DailySummary) !WeekRanges {
        if (summaries.len == 0) {
            return .{ .allocator = allocator, .items = &.{} };
        }

        var ranges = std.ArrayList(WeekRange).empty;
        defer ranges.deinit(allocator);
        var current_iso = try computeWeekStartIso(allocator, summaries[0].iso_date);
        errdefer allocator.free(current_iso);
        var current_display = try Model.formatDisplayDate(allocator, current_iso);
        errdefer allocator.free(current_display);
        var current = WeekRange{
            .iso_start = current_iso,
            .display_start = current_display,
            .start_index = 0,
            .end_index = 0,
        };

        var idx: usize = 0;
        while (idx < summaries.len) : (idx += 1) {
            const iso = try computeWeekStartIso(allocator, summaries[idx].iso_date);
            if (!std.mem.eql(u8, iso, current.iso_start)) {
                current.end_index = idx;
                try ranges.append(allocator, current);
                current_iso = iso;
                current_display = try Model.formatDisplayDate(allocator, current_iso);
                current = .{
                    .iso_start = current_iso,
                    .display_start = current_display,
                    .start_index = idx,
                    .end_index = idx,
                };
            } else {
                allocator.free(iso);
            }
        }
        current.end_index = summaries.len;
        try ranges.append(allocator, current);
        return .{ .allocator = allocator, .items = try ranges.toOwnedSlice(allocator) };
    }

    fn deinit(self: *WeekRanges) void {
        for (self.items) |range| {
            self.allocator.free(range.iso_start);
            self.allocator.free(range.display_start);
        }
        if (self.items.len != 0) self.allocator.free(self.items);
        self.* = undefined;
    }
};

const DateParts = struct {
    year: i32,
    month: u8,
    day: u8,
};

fn computeWeekStartIso(allocator: std.mem.Allocator, iso_date: []const u8) ![]u8 {
    const parts = try parseIsoDate(iso_date);
    const days = daysFromCivil(parts.year, parts.month, parts.day);
    const weekday = weekdayFromDays(days);
    const monday_days = days - @as(i64, weekday);
    const start_parts = civilFromDays(monday_days);
    return try formatIsoDate(allocator, start_parts);
}

fn parseIsoDate(iso_date: []const u8) !DateParts {
    if (iso_date.len < 10) return error.InvalidDate;
    const year = try std.fmt.parseInt(i32, iso_date[0..4], 10);
    const month = try std.fmt.parseInt(u8, iso_date[5..7], 10);
    const day = try std.fmt.parseInt(u8, iso_date[8..10], 10);
    return .{ .year = year, .month = month, .day = day };
}

fn daysFromCivil(year: i32, month_u8: u8, day_u8: u8) i64 {
    var y = year;
    var m = @as(i32, month_u8);
    if (m <= 2) {
        y -= 1;
        m += 12;
    }
    const d = @as(i32, day_u8);
    const era = if (y >= 0) @divTrunc(y, 400) else -@divTrunc(-y, 400) - 1;
    const yoe = y - era * 400;
    const doy = @divTrunc(153 * (m - 3) + 2, 5) + d - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + @divTrunc(yoe, 400) + doy;
    return @as(i64, era) * 146097 + @as(i64, doe) - 719468;
}

fn civilFromDays(z: i64) DateParts {
    const n = z + 719468;
    const era = if (n >= 0) @divTrunc(n, 146097) else -@divTrunc(-n, 146097) - 1;
    const doe = n - era * 146097;
    const yoe = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
    var year = @as(i32, @intCast(yoe + era * 400));
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const day = @as(u8, @intCast(doy - @divTrunc(153 * mp + 2, 5) + 1));
    const month = @as(u8, @intCast(if (mp < 10) mp + 3 else mp - 9));
    if (mp >= 10) year += 1;
    return .{ .year = year, .month = month, .day = day };
}

fn weekdayFromDays(days: i64) u8 {
    const offset = @mod((days + 3), 7);
    return @as(u8, @intCast(if (offset < 0) offset + 7 else offset));
}

fn formatIsoDate(allocator: std.mem.Allocator, parts: DateParts) ![]u8 {
    return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ parts.year, parts.month, parts.day });
}

fn buildPayload(
    allocator: std.mem.Allocator,
    timestamp: []const u8,
    machine: []const u8,
    hostname: []const u8,
    display_name: []const u8,
    codex_view: CodexReportView,
) ![]u8 {
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);
    var array_writer = ArrayWriter.init(&buffer, allocator);
    var stringify = std.json.Stringify{
        .writer = &array_writer.base,
        .options = .{ .whitespace = .indent_2 },
    };
    try stringify.write(PayloadView{
        .timestamp = timestamp,
        .machine_id = machine,
        .hostname = hostname,
        .display_name = display_name,
        .codex = codex_view,
    });
    try buffer.append(allocator, '\n');
    return buffer.toOwnedSlice(allocator);
}

const PayloadView = struct {
    timestamp: []const u8,
    machine_id: []const u8,
    hostname: []const u8,
    display_name: []const u8,
    codex: CodexReportView,

    pub fn jsonStringify(self: PayloadView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("timestamp");
        try jw.write(self.timestamp);
        try jw.objectField("machineId");
        try jw.write(self.machine_id);
        try jw.objectField("hostname");
        try jw.write(self.hostname);
        try jw.objectField("displayName");
        try jw.write(self.display_name);
        try jw.objectField("codex");
        try jw.write(self.codex);
        try jw.endObject();
    }
};

const CodexReportView = struct {
    allocator: std.mem.Allocator,
    sessions: *SessionAccumulator,
    summaries: []const Model.DailySummary,
    week_ranges: []const WeekRange,
    totals: *const Model.SummaryTotals,
    pricing: *const Model.PricingMap,

    pub fn jsonStringify(self: CodexReportView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("sessions");
        try jw.write(SessionsReportView{
            .allocator = self.allocator,
            .sessions = self.sessions,
            .pricing = self.pricing,
        });
        try jw.objectField("daily");
        try jw.write(DailyReportView{
            .allocator = self.allocator,
            .summaries = self.summaries,
            .totals = self.totals,
        });
        try jw.objectField("weekly");
        try jw.write(WeeklyReportView{
            .allocator = self.allocator,
            .summaries = self.summaries,
            .ranges = self.week_ranges,
            .totals = self.totals,
            .pricing = self.pricing,
        });
        try jw.endObject();
    }
};

const SessionsReportView = struct {
    allocator: std.mem.Allocator,
    sessions: *SessionAccumulator,
    pricing: *const Model.PricingMap,

    pub fn jsonStringify(self: SessionsReportView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("sessions");
        try jw.beginArray();
        var total_cost: f64 = 0;
        const keys = collectSortedKeys(SessionStats, self.allocator, &self.sessions.sessions) catch return error.WriteFailed;
        defer self.allocator.free(keys);
        for (keys) |key| {
            const stats = self.sessions.sessions.getPtr(key).?;
            var entry_cost: f64 = 0;
            try jw.write(SessionEntryView{
                .allocator = self.allocator,
                .stats = stats,
                .pricing = self.pricing,
                .cost_accumulator = &entry_cost,
            });
            total_cost += entry_cost;
        }
        try jw.endArray();
        try jw.objectField("totals");
        try jw.write(TotalsView{ .usage = self.sessions.totals, .cost = total_cost });
        try jw.endObject();
    }
};

const SessionEntryView = struct {
    allocator: std.mem.Allocator,
    stats: *SessionStats,
    pricing: *const Model.PricingMap,
    cost_accumulator: *f64,

    pub fn jsonStringify(self: SessionEntryView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("sessionId");
        try jw.write(self.stats.session_id);
        const slash = std.mem.lastIndexOfScalar(u8, self.stats.session_id, '/');
        const session_file = if (slash) |idx| self.stats.session_id[idx + 1 ..] else self.stats.session_id;
        const directory = if (slash) |idx| self.stats.session_id[0..idx] else "";
        try jw.objectField("sessionFile");
        try jw.write(session_file);
        try jw.objectField("directory");
        try jw.write(directory);
        try jw.objectField("lastActivity");
        try jw.write(self.stats.last_activity);
        try jw.objectField("inputTokens");
        try jw.write(self.stats.usage.input_tokens);
        try jw.objectField("cachedInputTokens");
        try jw.write(self.stats.usage.cached_input_tokens);
        try jw.objectField("outputTokens");
        try jw.write(self.stats.usage.output_tokens);
        try jw.objectField("reasoningOutputTokens");
        try jw.write(self.stats.usage.reasoning_output_tokens);
        try jw.objectField("totalTokens");
        try jw.write(self.stats.usage.total_tokens);
        try jw.objectField("models");
        try jw.write(SessionModelsView{
            .allocator = self.allocator,
            .models = &self.stats.models,
            .pricing = self.pricing,
            .cost_accumulator = self.cost_accumulator,
        });
        try jw.objectField("costUSD");
        try jw.write(self.cost_accumulator.*);
        try jw.endObject();
    }
};

const SessionModelsView = struct {
    allocator: std.mem.Allocator,
    models: *std.StringHashMap(ModelUsageStats),
    pricing: ?*const Model.PricingMap = null,
    cost_accumulator: ?*f64 = null,

    pub fn jsonStringify(self: SessionModelsView, jw: anytype) !void {
        try jw.beginObject();
        const keys = collectSortedKeys(ModelUsageStats, self.allocator, self.models) catch return error.WriteFailed;
        defer self.allocator.free(keys);
        for (keys) |key| {
            const entry = self.models.get(key).?;
            try jw.objectField(key);
            try jw.beginObject();
            try jw.objectField("inputTokens");
            try jw.write(entry.usage.input_tokens);
            try jw.objectField("cachedInputTokens");
            try jw.write(entry.usage.cached_input_tokens);
            try jw.objectField("outputTokens");
            try jw.write(entry.usage.output_tokens);
            try jw.objectField("reasoningOutputTokens");
            try jw.write(entry.usage.reasoning_output_tokens);
            try jw.objectField("totalTokens");
            try jw.write(entry.usage.total_tokens);
            try jw.objectField("isFallback");
            try jw.write(entry.is_fallback);
            try jw.endObject();
            if (self.pricing) |pricing_map| {
                if (pricing_map.get(key)) |price| {
                    if (self.cost_accumulator) |acc| {
                        acc.* += entry.usage.cost(price);
                    }
                }
            }
        }
        try jw.endObject();
    }
};

const DailyReportView = struct {
    allocator: std.mem.Allocator,
    summaries: []const Model.DailySummary,
    totals: *const Model.SummaryTotals,

    pub fn jsonStringify(self: DailyReportView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("daily");
        try jw.beginArray();
        for (self.summaries) |*summary| {
            try jw.write(DailyEntryView{
                .summary = summary,
            });
        }
        try jw.endArray();
        try jw.objectField("totals");
        try jw.write(TotalsView{ .usage = self.totals.usage, .cost = self.totals.cost_usd });
        try jw.endObject();
    }
};

const DailyEntryView = struct {
    summary: *const Model.DailySummary,

    pub fn jsonStringify(self: DailyEntryView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("date");
        try jw.write(self.summary.display_date);
        try jw.objectField("inputTokens");
        try jw.write(self.summary.usage.input_tokens);
        try jw.objectField("cachedInputTokens");
        try jw.write(self.summary.usage.cached_input_tokens);
        try jw.objectField("outputTokens");
        try jw.write(self.summary.usage.output_tokens);
        try jw.objectField("reasoningOutputTokens");
        try jw.write(self.summary.usage.reasoning_output_tokens);
        try jw.objectField("totalTokens");
        try jw.write(self.summary.usage.total_tokens);
        try jw.objectField("costUSD");
        try jw.write(self.summary.cost_usd);
        try jw.objectField("models");
        try jw.write(ModelListView{ .items = self.summary.models.items });
        try jw.endObject();
    }
};

const ModelListView = struct {
    items: []const Model.ModelSummary,

    pub fn jsonStringify(self: ModelListView, jw: anytype) !void {
        try jw.beginObject();
        for (self.items) |model| {
            try jw.objectField(model.name);
            try jw.beginObject();
            try jw.objectField("inputTokens");
            try jw.write(model.usage.input_tokens);
            try jw.objectField("cachedInputTokens");
            try jw.write(model.usage.cached_input_tokens);
            try jw.objectField("outputTokens");
            try jw.write(model.usage.output_tokens);
            try jw.objectField("reasoningOutputTokens");
            try jw.write(model.usage.reasoning_output_tokens);
            try jw.objectField("totalTokens");
            try jw.write(model.usage.total_tokens);
            try jw.objectField("isFallback");
            try jw.write(model.is_fallback);
            try jw.endObject();
        }
        try jw.endObject();
    }
};

const WeeklyReportView = struct {
    allocator: std.mem.Allocator,
    summaries: []const Model.DailySummary,
    ranges: []const WeekRange,
    totals: *const Model.SummaryTotals,
    pricing: *const Model.PricingMap,

    pub fn jsonStringify(self: WeeklyReportView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("weekly");
        try jw.beginArray();
        for (self.ranges) |range| {
            try jw.write(WeeklyEntryView{
                .allocator = self.allocator,
                .summaries = self.summaries,
                .range = range,
            });
        }
        try jw.endArray();
        try jw.objectField("totals");
        try jw.write(TotalsView{ .usage = self.totals.usage, .cost = self.totals.cost_usd });
        try jw.endObject();
    }
};

const WeeklyEntryView = struct {
    allocator: std.mem.Allocator,
    summaries: []const Model.DailySummary,
    range: WeekRange,

    pub fn jsonStringify(self: WeeklyEntryView, jw: anytype) !void {
        var aggregate = Model.TokenUsage{};
        var cost: f64 = 0;
        var model_map = std.StringHashMap(ModelUsageStats).init(self.allocator);
        defer {
            var it = model_map.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            model_map.deinit();
        }

        var idx = self.range.start_index;
        while (idx < self.range.end_index) : (idx += 1) {
            const summary = &self.summaries[idx];
            aggregate.add(summary.usage);
            cost += summary.cost_usd;
            for (summary.models.items) |model| {
                var gop = model_map.getOrPut(model.name) catch return error.WriteFailed;
                if (!gop.found_existing) {
                    const name_copy = self.allocator.dupe(u8, model.name) catch return error.WriteFailed;
                    gop.key_ptr.* = name_copy;
                    gop.value_ptr.* = .{ .usage = model.usage, .is_fallback = model.is_fallback };
                } else {
                    gop.value_ptr.usage.add(model.usage);
                    if (model.is_fallback) gop.value_ptr.is_fallback = true;
                }
            }
        }

        try jw.beginObject();
        try jw.objectField("weekStart");
        try jw.write(self.range.display_start);
        try jw.objectField("isoWeekStart");
        try jw.write(self.range.iso_start);
        try jw.objectField("inputTokens");
        try jw.write(aggregate.input_tokens);
        try jw.objectField("cachedInputTokens");
        try jw.write(aggregate.cached_input_tokens);
        try jw.objectField("outputTokens");
        try jw.write(aggregate.output_tokens);
        try jw.objectField("reasoningOutputTokens");
        try jw.write(aggregate.reasoning_output_tokens);
        try jw.objectField("totalTokens");
        try jw.write(aggregate.total_tokens);
        try jw.objectField("costUSD");
        try jw.write(cost);
        try jw.objectField("models");
        try jw.write(SessionModelsView{
            .allocator = self.allocator,
            .models = &model_map,
            .pricing = null,
            .cost_accumulator = null,
        });
        try jw.endObject();
    }
};

const TotalsView = struct {
    usage: Model.TokenUsage,
    cost: f64,

    pub fn jsonStringify(self: TotalsView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("inputTokens");
        try jw.write(self.usage.input_tokens);
        try jw.objectField("cachedInputTokens");
        try jw.write(self.usage.cached_input_tokens);
        try jw.objectField("outputTokens");
        try jw.write(self.usage.output_tokens);
        try jw.objectField("reasoningOutputTokens");
        try jw.write(self.usage.reasoning_output_tokens);
        try jw.objectField("totalTokens");
        try jw.write(self.usage.total_tokens);
        try jw.objectField("costUSD");
        try jw.write(self.cost);
        try jw.endObject();
    }
};

fn collectSortedKeys(
    comptime T: type,
    allocator: std.mem.Allocator,
    map: *const std.StringHashMap(T),
) ![]const []const u8 {
    var list = try allocator.alloc([]const u8, map.count());
    var it = map.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| {
        list[idx] = entry.key_ptr.*;
        idx += 1;
    }
    std.sort.pdq([]const u8, list, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);
    return list;
}

const HttpResponse = struct {
    status: std.http.Status,
    body: []u8,

    fn deinit(self: *HttpResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

fn sendPayload(
    allocator: std.mem.Allocator,
    endpoint: []const u8,
    api_key: []const u8,
    payload: []u8,
) !HttpResponse {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();

    var client = std.http.Client{
        .allocator = allocator,
        .io = io_single.io(),
    };
    defer client.deinit();

    const uri = try std.Uri.parse(endpoint);
    var extra_headers = [_]std.http.Header{
        .{ .name = "X-API-Key", .value = api_key },
    };

    var request = try client.request(.POST, uri, .{
        .headers = .{
            .content_type = .{ .override = "application/json" },
        },
        .extra_headers = &extra_headers,
    });
    defer request.deinit();

    try request.sendBodyComplete(payload);
    var response = try request.receiveHead(&.{});

    var transfer_buffer: [4096]u8 = undefined;
    const reader = response.reader(&transfer_buffer);
    const body = try reader.allocRemaining(allocator, std.Io.Limit.limited(1024 * 1024));

    return .{
        .status = response.head.status,
        .body = body,
    };
}

fn buildEndpoint(allocator: std.mem.Allocator, base: []const u8) ![]u8 {
    const trimmed = trimTrailingSlash(base);
    return std.fmt.allocPrint(allocator, "{s}/api/usage/report", .{trimmed});
}

fn trimTrailingSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn handleResponse(response: HttpResponse) void {
    const body_trimmed = std.mem.trim(u8, response.body, " \n\r\t");
    switch (response.status) {
        .ok => {
            if (extractField(body_trimmed, "\"user\":\"")) |user| {
                const machine = extractField(body_trimmed, "\"machineId\":\"") orelse "";
                std.debug.print("✅ Codex usage reported for {s}\n", .{user});
                if (machine.len > 0) std.debug.print("   Machine: {s}\n", .{machine});
                std.debug.print("   Sources: Codex\n", .{});
                return;
            }
            std.debug.print("✅ Codex usage reported successfully\n", .{});
        },
        .unauthorized => {
            std.debug.print("❌ Authentication failed: Invalid or inactive API key\n", .{});
            std.debug.print("   Please check your DASHBOARD_API_KEY\n", .{});
        },
        .unprocessable_entity => {
            std.debug.print("❌ Data validation error. Please check Codex data formatting.\n", .{});
            if (body_trimmed.len > 0) std.debug.print("   Error: {s}\n", .{body_trimmed});
        },
        .internal_server_error => {
            std.debug.print("⚠️  Server error. Please try again later.\n", .{});
            if (body_trimmed.len > 0) std.debug.print("   Error: {s}\n", .{body_trimmed});
        },
        else => {
            std.debug.print(
                "⚠️  Failed to report usage (HTTP {d})\n",
                .{@intFromEnum(response.status)},
            );
            if (body_trimmed.len > 0) std.debug.print("   Response: {s}\n", .{body_trimmed});
        },
    }
}

fn extractField(body: []const u8, prefix: []const u8) ?[]const u8 {
    if (std.mem.indexOf(u8, body, prefix)) |start| {
        const idx = start + prefix.len;
        var end = idx;
        while (end < body.len and body[end] != '"') : (end += 1) {}
        if (end > idx) return body[idx..end];
    }
    return null;
}

fn commandSucceeded(term: std.process.Child.Term) bool {
    return switch (term) {
        .Exited => |code| code == 0,
        else => false,
    };
}
