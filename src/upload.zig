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

    var consumer_ctx = UploadIngestContext{
        .builder = &summary_builder,
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

    const codex_view = CodexReportView{
        .allocator = allocator,
        .summaries = summaries,
        .totals = &totals,
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

const UploadIngestContext = struct {
    builder: *Model.SummaryBuilder,
};

fn uploadIngest(
    ctx_ptr: *anyopaque,
    allocator: std.mem.Allocator,
    event: *Model.TokenUsageEvent,
    filters: Model.DateFilters,
) anyerror!void {
    const ctx = @as(*UploadIngestContext, @ptrCast(@alignCast(ctx_ptr)));
    try ctx.builder.ingest(allocator, event, filters);
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
    summaries: []const Model.DailySummary,
    totals: *const Model.SummaryTotals,

    pub fn jsonStringify(self: CodexReportView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("days");
        try jw.beginArray();
        for (self.summaries) |*summary| {
            try jw.write(DailyEntryView{ .summary = summary });
        }
        try jw.endArray();
        try jw.objectField("total");
        try jw.write(TotalsView{
            .usage = self.totals.usage,
            .display_input_tokens = self.totals.display_input_tokens,
            .cost = self.totals.cost_usd,
            .missing = self.totals.missing_pricing.items,
        });
        try jw.endObject();
    }
};

const DailyEntryView = struct {
    summary: *const Model.DailySummary,

    pub fn jsonStringify(self: DailyEntryView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("date");
        try jw.write(self.summary.display_date);
        try jw.objectField("isoDate");
        try jw.write(self.summary.iso_date);
        try writeUsageFields(jw, self.summary.usage, self.summary.display_input_tokens);
        try jw.objectField("costUSD");
        try jw.write(self.summary.cost_usd);
        try jw.objectField("models");
        try jw.write(ModelArrayView{ .items = self.summary.models.items });
        try jw.objectField("missingPricing");
        try jw.write(self.summary.missing_pricing.items);
        try jw.endObject();
    }
};

const ModelArrayView = struct {
    items: []const Model.ModelSummary,

    pub fn jsonStringify(self: ModelArrayView, jw: anytype) !void {
        try jw.beginArray();
        for (self.items) |*model| {
            try jw.write(ModelEntryView{ .model = model });
        }
        try jw.endArray();
    }
};

const ModelEntryView = struct {
    model: *const Model.ModelSummary,

    pub fn jsonStringify(self: ModelEntryView, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("name");
        try jw.write(self.model.name);
        try jw.objectField("displayName");
        if (self.model.is_fallback) {
            var buffer: [256]u8 = undefined;
            const display = std.fmt.bufPrint(&buffer, "{s} (fallback)", .{self.model.name}) catch self.model.name;
            try jw.write(display);
        } else {
            try jw.write(self.model.name);
        }
        try jw.objectField("isFallback");
        try jw.write(self.model.is_fallback);
        try writeUsageFields(jw, self.model.usage, self.model.display_input_tokens);
        try jw.objectField("costUSD");
        try jw.write(self.model.cost_usd);
        try jw.objectField("pricingAvailable");
        try jw.write(self.model.pricing_available);
        try jw.endObject();
    }
};

const TotalsView = struct {
    usage: Model.TokenUsage,
    display_input_tokens: u64,
    cost: f64,
    missing: []const []const u8,

    pub fn jsonStringify(self: TotalsView, jw: anytype) !void {
        try jw.beginObject();
        try writeUsageFields(jw, self.usage, self.display_input_tokens);
        try jw.objectField("costUSD");
        try jw.write(self.cost);
        try jw.objectField("missingPricing");
        try jw.write(self.missing);
        try jw.endObject();
    }
};

fn writeUsageFields(jw: anytype, usage: Model.TokenUsage, display_input: u64) !void {
    try jw.objectField("inputTokens");
    try jw.write(display_input);
    try jw.objectField("cachedInputTokens");
    try jw.write(usage.cached_input_tokens);
    try jw.objectField("outputTokens");
    try jw.write(usage.output_tokens);
    try jw.objectField("reasoningOutputTokens");
    try jw.write(usage.reasoning_output_tokens);
    try jw.objectField("totalTokens");
    try jw.write(usage.total_tokens);
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
