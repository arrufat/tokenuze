const std = @import("std");
const io_util = @import("io_util.zig");
const timeutil = @import("time.zig");

pub const million = 1_000_000.0;
pub const pricing_url = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

pub const UsageFieldVisibility = struct {
    cache_creation: bool = true,
    cache_read: bool = true,
};

pub const OutputFormat = enum {
    table,
    json,
};

pub const DateFilters = struct {
    since: ?[10]u8 = null,
    until: ?[10]u8 = null,
    pretty_output: bool = false,
    output_format: OutputFormat = .table,
    timezone_offset_minutes: i16 = @intCast(timeutil.default_timezone_offset_minutes),
};

pub const ParseDateError = error{
    InvalidFormat,
    InvalidDate,
};

pub const IngestError = error{
    OutOfMemory,
    InvalidDate,
    InvalidCharacter,
    Overflow,
};

pub fn parseFilterDate(input: []const u8) ParseDateError![10]u8 {
    if (input.len != 8) return error.InvalidFormat;

    _ = std.fmt.parseInt(u16, input[0..4], 10) catch return error.InvalidFormat;
    const month = std.fmt.parseInt(u8, input[4..6], 10) catch return error.InvalidFormat;
    const day = std.fmt.parseInt(u8, input[6..8], 10) catch return error.InvalidFormat;

    if (month == 0 or month > 12) return error.InvalidDate;
    if (day == 0 or day > 31) return error.InvalidDate;

    var buffer: [10]u8 = undefined;
    std.mem.copyForwards(u8, buffer[0..4], input[0..4]);
    buffer[4] = '-';
    std.mem.copyForwards(u8, buffer[5..7], input[4..6]);
    buffer[7] = '-';
    std.mem.copyForwards(u8, buffer[8..10], input[6..8]);

    return buffer;
}

pub const TokenUsage = struct {
    input_tokens: u64 = 0,
    cache_creation_input_tokens: u64 = 0,
    cached_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    reasoning_output_tokens: u64 = 0,
    total_tokens: u64 = 0,

    pub fn add(self: *TokenUsage, other: TokenUsage) void {
        self.input_tokens += other.input_tokens;
        self.cache_creation_input_tokens += other.cache_creation_input_tokens;
        self.cached_input_tokens += other.cached_input_tokens;
        self.output_tokens += other.output_tokens;
        self.reasoning_output_tokens += other.reasoning_output_tokens;
        self.total_tokens += other.total_tokens;
    }

    pub fn cost(self: TokenUsage, pricing: ModelPricing) f64 {
        const overlap = if (self.input_tokens >= self.cached_input_tokens) self.cached_input_tokens else 0;
        const non_cached = self.input_tokens - overlap;
        const cached = self.cached_input_tokens;

        const cache_creation_cost = (@as(f64, @floatFromInt(self.cache_creation_input_tokens)) / million) * pricing.cache_creation_cost_per_m;
        const input_cost = (@as(f64, @floatFromInt(non_cached)) / million) * pricing.input_cost_per_m;
        const cached_cost = (@as(f64, @floatFromInt(cached)) / million) * pricing.cached_input_cost_per_m;
        const output_cost = (@as(f64, @floatFromInt(self.output_tokens)) / million) * pricing.output_cost_per_m;

        return cache_creation_cost + input_cost + cached_cost + output_cost;
    }

    pub fn fromRaw(raw: RawTokenUsage) TokenUsage {
        return .{
            .input_tokens = raw.input_tokens,
            .cache_creation_input_tokens = raw.cache_creation_input_tokens,
            .cached_input_tokens = raw.cached_input_tokens,
            .output_tokens = raw.output_tokens,
            .reasoning_output_tokens = raw.reasoning_output_tokens,
            .total_tokens = if (raw.total_tokens > 0)
                raw.total_tokens
            else
                raw.input_tokens + raw.cache_creation_input_tokens + raw.cached_input_tokens + raw.output_tokens,
        };
    }

    pub fn deltaFrom(current: RawTokenUsage, previous: ?RawTokenUsage) TokenUsage {
        const delta_raw = rawUsageDelta(current, previous);
        return TokenUsage.fromRaw(delta_raw);
    }
};

pub const RawTokenUsage = struct {
    input_tokens: u64 = 0,
    cache_creation_input_tokens: u64 = 0,
    cached_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    reasoning_output_tokens: u64 = 0,
    total_tokens: u64 = 0,
};

pub const UsageAccumulator = struct {
    raw: RawTokenUsage = .{},
    cached_direct: ?u64 = null,
    cached_fallback: ?u64 = null,

    pub fn applyField(self: *UsageAccumulator, field: UsageField, value: u64) void {
        switch (field) {
            .input_tokens => self.raw.input_tokens = value,
            .cache_creation_input_tokens => self.raw.cache_creation_input_tokens = value,
            .cached_input_tokens => self.cached_direct = value,
            .cache_read_input_tokens => self.cached_fallback = value,
            .output_tokens => self.raw.output_tokens = value,
            .reasoning_output_tokens => self.raw.reasoning_output_tokens = value,
            .total_tokens => self.raw.total_tokens = value,
        }
    }

    pub fn addField(self: *UsageAccumulator, field: UsageField, value: u64) void {
        if (value == 0) return;
        switch (field) {
            .input_tokens => self.raw.input_tokens = self.raw.input_tokens +| value,
            .cache_creation_input_tokens => self.raw.cache_creation_input_tokens = self.raw.cache_creation_input_tokens +| value,
            .cached_input_tokens => {
                const current = self.cached_direct orelse 0;
                self.cached_direct = current +| value;
            },
            .cache_read_input_tokens => {
                const current = self.cached_fallback orelse 0;
                self.cached_fallback = current +| value;
            },
            .output_tokens => self.raw.output_tokens = self.raw.output_tokens +| value,
            .reasoning_output_tokens => self.raw.reasoning_output_tokens = self.raw.reasoning_output_tokens +| value,
            .total_tokens => self.raw.total_tokens = self.raw.total_tokens +| value,
        }
    }

    pub fn finalize(self: *UsageAccumulator) RawTokenUsage {
        if (self.cached_direct) |direct| {
            if (direct > 0) {
                self.raw.cached_input_tokens = direct;
            } else if (self.cached_fallback) |fallback| {
                self.raw.cached_input_tokens = fallback;
            }
        } else if (self.cached_fallback) |fallback| {
            self.raw.cached_input_tokens = fallback;
        }
        return self.raw;
    }
};

pub const UsageField = enum {
    input_tokens,
    cache_creation_input_tokens,
    cached_input_tokens,
    cache_read_input_tokens,
    output_tokens,
    reasoning_output_tokens,
    total_tokens,
};

const usage_field_map = std.StaticStringMap(UsageField).initComptime(.{
    .{ "input_tokens", .input_tokens },
    .{ "cache_creation_input_tokens", .cache_creation_input_tokens },
    .{ "cached_input_tokens", .cached_input_tokens },
    .{ "cache_read_input_tokens", .cache_read_input_tokens },
    .{ "output_tokens", .output_tokens },
    .{ "reasoning_output_tokens", .reasoning_output_tokens },
    .{ "total_tokens", .total_tokens },
});

pub fn usageFieldForKey(key: []const u8) ?UsageField {
    return usage_field_map.get(key);
}

pub fn parseTokenNumber(slice: []const u8) u64 {
    if (slice.len == 0) return 0;
    if (std.mem.findScalar(u8, slice, '.')) |_| {
        const parsed = std.fmt.parseFloat(f64, slice) catch return 0;
        return if (parsed >= 0)
            @as(u64, @intFromFloat(std.math.floor(parsed)))
        else
            0;
    }
    return std.fmt.parseInt(u64, slice, 10) catch 0;
}

pub fn writeUsageJsonFields(
    jw: *std.json.Stringify,
    usage: TokenUsage,
    display_input_override: ?u64,
    visibility: UsageFieldVisibility,
) !void {
    const input_tokens = display_input_override orelse usage.input_tokens;
    try jw.objectField("inputTokens");
    try jw.write(input_tokens);
    if (visibility.cache_creation) {
        try jw.objectField("cacheCreationInputTokens");
        try jw.write(usage.cache_creation_input_tokens);
    }
    if (visibility.cache_read) {
        try jw.objectField("cachedInputTokens");
        try jw.write(usage.cached_input_tokens);
    }
    try jw.objectField("outputTokens");
    try jw.write(usage.output_tokens);
    try jw.objectField("reasoningOutputTokens");
    try jw.write(usage.reasoning_output_tokens);
    try jw.objectField("totalTokens");
    try jw.write(usage.total_tokens);
}

test "writeUsageJsonFields omits cache fields when hidden" {
    const usage = TokenUsage{
        .input_tokens = 10,
        .cache_creation_input_tokens = 5,
        .cached_input_tokens = 3,
        .output_tokens = 7,
        .reasoning_output_tokens = 0,
        .total_tokens = 25,
    };
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(std.testing.allocator);
    var writer_state = io_util.ArrayWriter.init(&buffer, std.testing.allocator);
    var stringify = std.json.Stringify{ .writer = writer_state.writer(), .options = .{} };
    try stringify.beginObject();
    try writeUsageJsonFields(&stringify, usage, null, .{ .cache_creation = false, .cache_read = false });
    try stringify.endObject();
    try std.testing.expect(std.mem.find(u8, buffer.items, "cacheCreationInputTokens") == null);
    try std.testing.expect(std.mem.find(u8, buffer.items, "cachedInputTokens") == null);
}

pub const TokenUsageEvent = struct {
    session_id: []const u8,
    timestamp: []const u8,
    local_iso_date: [10]u8,
    model: []const u8,
    usage: TokenUsage,
    is_fallback: bool,
    display_input_tokens: u64,
};

pub const ModelPricing = struct {
    input_cost_per_m: f64,
    cache_creation_cost_per_m: f64,
    cached_input_cost_per_m: f64,
    output_cost_per_m: f64,
};

pub const PricingMap = std.StringHashMap(ModelPricing);

const pricing_candidate_prefixes = [_][]const u8{
    "anthropic.",
    "anthropic/",
    "bedrock.",
    "claude-",
    "claude-3-",
    "claude-3-5-",
    "openrouter/anthropic/",
    "openrouter/openai/",
    "vertex_ai/",
};

const pricing_aliases = [_]struct { alias: []const u8, target: []const u8 }{
    // Gemini names as displayed in Zed
    .{ .alias = "gemini 3 pro", .target = "gemini-3-pro" },
    .{ .alias = "gemini 2.5 pro", .target = "gemini-2.5-pro" },
    .{ .alias = "gemini 2.5 flash", .target = "gemini-2.5-flash" },
    .{ .alias = "gemini 2.5 flash-lite", .target = "gemini-2.5-flash-lite" },
    .{ .alias = "gemini 2.5 flash-lite preview", .target = "gemini-2.5-flash-lite" },
    .{ .alias = "gemini 2.0 flash", .target = "gemini-2.0-flash" },
    .{ .alias = "gemini 2.0 flash-lite", .target = "gemini-2.0-flash-lite" },
    .{ .alias = "gemini 1.5 pro", .target = "gemini-1.5-pro" },
    .{ .alias = "gemini 1.5 flash", .target = "gemini-1.5-flash" },
    .{ .alias = "gemini 1.5 flash-8b", .target = "gemini-1.5-flash-8b" },
};

pub fn deinitPricingMap(map: *PricingMap, allocator: std.mem.Allocator) void {
    var iterator = map.iterator();
    while (iterator.next()) |entry| {
        allocator.free(entry.key_ptr.*);
    }
    map.deinit();
}

pub const ModelSummary = struct {
    name: []const u8,
    is_fallback: bool,
    pricing_available: bool,
    cost_usd: f64,
    usage: TokenUsage,
    display_input_tokens: u64 = 0,
};

pub const DailySummary = struct {
    iso_date: []const u8,
    display_date: []const u8,
    usage: TokenUsage,
    display_input_tokens: u64,
    cost_usd: f64,
    models: std.ArrayListUnmanaged(ModelSummary),
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init(allocator: std.mem.Allocator, iso_date: []const u8, display_date: []const u8) DailySummary {
        _ = allocator;
        return .{
            .iso_date = iso_date,
            .display_date = display_date,
            .usage = .{},
            .display_input_tokens = 0,
            .cost_usd = 0,
            .models = .{},
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *DailySummary, allocator: std.mem.Allocator) void {
        for (self.models.items) |model| {
            allocator.free(model.name);
        }
        self.models.deinit(allocator);
        self.missing_pricing.deinit(allocator);
        allocator.free(self.iso_date);
        allocator.free(self.display_date);
    }
};

pub const SummaryTotals = struct {
    usage: TokenUsage = .{},
    display_input_tokens: u64 = 0,
    cost_usd: f64 = 0,
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init() SummaryTotals {
        return .{
            .usage = .{},
            .display_input_tokens = 0,
            .cost_usd = 0,
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *SummaryTotals, allocator: std.mem.Allocator) void {
        for (self.missing_pricing.items) |name| {
            allocator.free(name);
        }
        self.missing_pricing.deinit(allocator);
    }
};

pub const SessionRecorder = struct {
    sessions: std.StringHashMap(SessionEntry),
    totals: TokenUsage = .{},
    display_total_input_tokens: u64 = 0,
    total_cost_usd: f64 = 0,

    pub fn init(allocator: std.mem.Allocator) SessionRecorder {
        return .{
            .sessions = std.StringHashMap(SessionEntry).init(allocator),
            .totals = .{},
            .total_cost_usd = 0,
        };
    }

    pub fn deinit(self: *SessionRecorder, allocator: std.mem.Allocator) void {
        var iterator = self.sessions.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        self.sessions.deinit();
        self.* = undefined;
    }

    pub fn ingest(self: *SessionRecorder, allocator: std.mem.Allocator, event: *const TokenUsageEvent) !void {
        var gop = try self.sessions.getOrPut(event.session_id);
        var session = gop.value_ptr;
        var owned_id: ?[]const u8 = null;

        if (!gop.found_existing) {
            const duplicated = try allocator.dupe(u8, event.session_id);
            owned_id = duplicated;
            gop.key_ptr.* = duplicated;
            const parts = splitSessionId(duplicated);
            session.* = SessionEntry.init(parts.session_id, parts.directory, parts.session_file);
        }

        session.recordEvent(allocator, event) catch |err| {
            if (!gop.found_existing) {
                const key = owned_id orelse gop.key_ptr.*;
                if (self.sessions.fetchRemove(key)) |removed| {
                    var entry = removed.value;
                    entry.deinit(allocator);
                    allocator.free(removed.key);
                }
            }
            return err;
        };
        self.totals.add(event.usage);
    }

    pub fn applyPricing(self: *SessionRecorder, pricing: *PricingMap) void {
        self.total_cost_usd = 0;
        var iterator = self.sessions.iterator();
        while (iterator.next()) |entry| {
            var session = entry.value_ptr;
            session.applyPricing(pricing);
            self.total_cost_usd += session.cost_usd;
        }
    }

    pub fn renderJson(self: *const SessionRecorder, allocator: std.mem.Allocator, pretty: bool) ![]u8 {
        var buffer = std.ArrayList(u8).empty;
        defer buffer.deinit(allocator);
        var writer_state = io_util.ArrayWriter.init(&buffer, allocator);
        var stringify = std.json.Stringify{
            .writer = writer_state.writer(),
            .options = .{
                .whitespace = if (pretty) .indent_2 else .minified,
            },
        };
        try stringify.beginObject();
        try stringify.objectField("sessions");
        try self.writeSessionsArray(allocator, &stringify);
        try stringify.objectField("totals");
        try writeUsageObject(&stringify, self.totals, self.total_cost_usd, self.display_total_input_tokens);
        try stringify.endObject();
        return buffer.toOwnedSlice(allocator);
    }

    fn writeSessionsArray(self: *const SessionRecorder, allocator: std.mem.Allocator, jw: *std.json.Stringify) !void {
        var pointers = try self.sortedSessions(allocator);
        defer pointers.deinit(allocator);
        try jw.beginArray();
        for (pointers.items) |session| {
            try session.writeJson(jw);
        }
        try jw.endArray();
    }

    pub fn sortedSessions(
        self: *const SessionRecorder,
        allocator: std.mem.Allocator,
    ) !std.ArrayListUnmanaged(*const SessionEntry) {
        var list = std.ArrayListUnmanaged(*const SessionEntry){};
        errdefer list.deinit(allocator);
        var iterator = self.sessions.iterator();
        while (iterator.next()) |entry| {
            try list.append(allocator, entry.value_ptr);
        }
        std.sort.pdq(*const SessionEntry, list.items, {}, sessionLessThan);
        return list;
    }

    fn sessionLessThan(_: void, lhs: *const SessionEntry, rhs: *const SessionEntry) bool {
        return std.mem.lessThan(u8, lhs.session_id, rhs.session_id);
    }

    fn writeUsageObject(jw: *std.json.Stringify, usage: TokenUsage, cost: f64, display_input_tokens: u64) !void {
        try jw.beginObject();
        const display_override: ?u64 = if (display_input_tokens > 0) display_input_tokens else null;
        try writeUsageJsonFields(jw, usage, display_override, .{});
        try jw.objectField("costUSD");
        try jw.write(cost);
        try jw.endObject();
    }

    fn splitSessionId(session_id: []const u8) struct {
        session_id: []const u8,
        directory: []const u8,
        session_file: []const u8,
    } {
        if (std.mem.lastIndexOfScalar(u8, session_id, '/')) |last_slash| {
            const directory = session_id[0..last_slash];
            const file = session_id[last_slash + 1 ..];
            return .{ .session_id = session_id, .directory = directory, .session_file = file };
        }
        return .{ .session_id = session_id, .directory = "", .session_file = session_id };
    }

    pub const SessionEntry = struct {
        session_id: []const u8,
        directory: []const u8,
        session_file: []const u8,
        last_activity: ?[]const u8 = null,
        usage: TokenUsage = .{},
        models: std.ArrayListUnmanaged(SessionModel) = .{},
        cost_usd: f64 = 0,

        fn init(session_id: []const u8, directory: []const u8, session_file: []const u8) SessionEntry {
            return .{
                .session_id = session_id,
                .directory = directory,
                .session_file = session_file,
                .last_activity = null,
                .usage = .{},
                .models = .{},
                .cost_usd = 0,
            };
        }

        fn deinit(self: *SessionEntry, allocator: std.mem.Allocator) void {
            if (self.last_activity) |timestamp| {
                allocator.free(timestamp);
            }
            for (self.models.items) |model| {
                allocator.free(model.name);
            }
            self.models.deinit(allocator);
            self.* = undefined;
        }

        fn recordEvent(self: *SessionEntry, allocator: std.mem.Allocator, event: *const TokenUsageEvent) !void {
            self.usage.add(event.usage);
            try self.updateLastActivity(allocator, event.timestamp);
            try self.applyModelUsage(allocator, event.model, event.usage, event.is_fallback);
        }

        fn applyPricing(self: *SessionEntry, pricing: *PricingMap) void {
            self.cost_usd = 0;
            for (self.models.items) |*model| {
                if (pricing.get(model.name)) |rate| {
                    model.pricing_available = true;
                    model.cost_usd = model.usage.cost(rate);
                    self.cost_usd += model.cost_usd;
                } else {
                    model.pricing_available = false;
                    model.cost_usd = 0;
                }
            }
            std.sort.pdq(SessionModel, self.models.items, {}, sessionModelLessThan);
        }

        fn updateLastActivity(self: *SessionEntry, allocator: std.mem.Allocator, timestamp: []const u8) !void {
            const owned = try allocator.dupe(u8, timestamp);
            if (self.last_activity) |existing| {
                if (std.mem.lessThan(u8, existing, owned)) {
                    allocator.free(existing);
                    self.last_activity = owned;
                } else {
                    allocator.free(owned);
                }
            } else {
                self.last_activity = owned;
            }
        }

        fn applyModelUsage(
            self: *SessionEntry,
            allocator: std.mem.Allocator,
            model_name: []const u8,
            usage: TokenUsage,
            is_fallback: bool,
        ) !void {
            for (self.models.items) |*existing| {
                if (std.mem.eql(u8, existing.name, model_name)) {
                    existing.usage.add(usage);
                    if (is_fallback) existing.is_fallback = true;
                    return;
                }
            }

            const owned_name = try allocator.dupe(u8, model_name);
            errdefer allocator.free(owned_name);
            try self.models.append(allocator, .{
                .name = owned_name,
                .usage = usage,
                .is_fallback = is_fallback,
                .pricing_available = false,
                .cost_usd = 0,
            });
        }

        fn writeJson(self: *const SessionEntry, jw: *std.json.Stringify) !void {
            try jw.beginObject();
            try jw.objectField("sessionId");
            try jw.write(self.session_id);
            try jw.objectField("lastActivity");
            if (self.last_activity) |timestamp| {
                try jw.write(timestamp);
            } else {
                try jw.write(null);
            }
            try jw.objectField("sessionFile");
            try jw.write(self.session_file);
            try jw.objectField("directory");
            try jw.write(self.directory);
            try writeUsageJsonFields(jw, self.usage, null, .{});
            try jw.objectField("costUSD");
            try jw.write(self.cost_usd);
            try jw.objectField("models");
            try jw.beginObject();
            for (self.models.items) |model| {
                try jw.objectField(model.name);
                try jw.beginObject();
                try writeUsageJsonFields(jw, model.usage, null, .{});
                try jw.objectField("costUSD");
                try jw.write(model.cost_usd);
                try jw.objectField("pricingAvailable");
                try jw.write(model.pricing_available);
                try jw.objectField("isFallback");
                try jw.write(model.is_fallback);
                try jw.endObject();
            }
            try jw.endObject();
            try jw.endObject();
        }
    };

    pub const SessionModel = struct {
        name: []const u8,
        usage: TokenUsage,
        is_fallback: bool,
        pricing_available: bool,
        cost_usd: f64,
    };

    fn sessionModelLessThan(_: void, lhs: SessionModel, rhs: SessionModel) bool {
        return std.mem.lessThan(u8, lhs.name, rhs.name);
    }
};

pub const SummaryBuilder = struct {
    summaries: std.ArrayListUnmanaged(DailySummary) = .{},
    date_index: std.StringHashMap(usize),
    event_count: usize = 0,
    session_recorder: ?*SessionRecorder = null,

    pub fn init(allocator: std.mem.Allocator) SummaryBuilder {
        return .{
            .summaries = .{},
            .date_index = std.StringHashMap(usize).init(allocator),
            .event_count = 0,
            .session_recorder = null,
        };
    }

    pub fn deinit(self: *SummaryBuilder, allocator: std.mem.Allocator) void {
        for (self.summaries.items) |*summary| {
            summary.deinit(allocator);
        }
        self.summaries.deinit(allocator);
        self.date_index.deinit();
        self.session_recorder = null;
    }

    pub fn attachSessionRecorder(self: *SummaryBuilder, recorder: *SessionRecorder) void {
        self.session_recorder = recorder;
    }

    pub fn ingest(
        self: *SummaryBuilder,
        allocator: std.mem.Allocator,
        event: *const TokenUsageEvent,
        filters: DateFilters,
    ) IngestError!void {
        const iso_slice = event.local_iso_date[0..];
        if (!dateWithinFilters(filters, iso_slice)) return;

        self.event_count += 1;

        if (self.date_index.get(iso_slice)) |idx| {
            try updateSummary(&self.summaries.items[idx], allocator, event);
        } else {
            const iso_copy = try allocator.dupe(u8, iso_slice);
            const display = try formatDisplayDate(allocator, iso_copy);
            try self.summaries.append(allocator, DailySummary.init(allocator, iso_copy, display));
            const summary_idx = self.summaries.items.len - 1;
            try self.date_index.put(self.summaries.items[summary_idx].iso_date, summary_idx);
            try updateSummary(&self.summaries.items[summary_idx], allocator, event);
        }
        if (self.session_recorder) |recorder| {
            try recorder.ingest(allocator, event);
        }
    }

    pub fn items(self: *SummaryBuilder) []DailySummary {
        return self.summaries.items;
    }

    pub fn len(self: SummaryBuilder) usize {
        return self.summaries.items.len;
    }

    pub fn eventCount(self: SummaryBuilder) usize {
        return self.event_count;
    }
};

pub fn applyPricing(
    allocator: std.mem.Allocator,
    summary: *DailySummary,
    pricing_map: *PricingMap,
    missing_set: *std.StringHashMap(u8),
) void {
    summary.cost_usd = 0;
    summary.missing_pricing.clearRetainingCapacity();

    for (summary.models.items) |*model| {
        if (resolveModelPricing(allocator, pricing_map, model.name)) |rate| {
            model.pricing_available = true;
            model.cost_usd = model.usage.cost(rate);
            summary.cost_usd += model.cost_usd;
        } else {
            model.pricing_available = false;
            model.cost_usd = 0;
            _ = missing_set.put(model.name, 1) catch {};
            appendUniqueString(allocator, &summary.missing_pricing, model.name) catch {};
        }
    }
}

fn resolveModelPricing(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    model_name: []const u8,
) ?ModelPricing {
    if (pricingAliasTarget(model_name)) |target| {
        if (resolveWithName(allocator, pricing_map, target, model_name)) |rate| return rate;
    }

    if (resolveWithName(allocator, pricing_map, model_name, model_name)) |rate|
        return rate;

    var variants: [2]?[]u8 = .{ null, null };
    var variant_count: usize = 0;

    variants[variant_count] = createDateVariant(allocator, model_name, '-', '@') catch null;
    if (variants[variant_count]) |_| variant_count += 1;
    variants[variant_count] = createDateVariant(allocator, model_name, '@', '-') catch null;
    if (variants[variant_count]) |_| variant_count += 1;

    defer {
        for (variants) |maybe_variant| {
            if (maybe_variant) |variant| allocator.free(variant);
        }
    }

    for (variants[0..variant_count]) |maybe_variant| {
        if (maybe_variant) |variant| {
            if (resolveWithName(allocator, pricing_map, variant, model_name)) |rate|
                return rate;
        }
    }

    return null;
}

fn pricingAliasTarget(name: []const u8) ?[]const u8 {
    for (pricing_aliases) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.alias, name)) return entry.target;
    }
    return null;
}

fn resolveWithName(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?ModelPricing {
    if (pricing_map.get(lookup_name)) |rate| {
        if (!std.mem.eql(u8, alias_name, lookup_name)) {
            tryCachePricingAlias(allocator, pricing_map, alias_name, rate);
        }
        return rate;
    }

    if (lookupWithPrefixes(allocator, pricing_map, lookup_name, alias_name)) |rate| return rate;
    return lookupBySubstring(allocator, pricing_map, lookup_name, alias_name);
}

fn lookupWithPrefixes(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?ModelPricing {
    for (pricing_candidate_prefixes) |prefix| {
        const candidate = std.fmt.allocPrint(allocator, "{s}{s}", .{ prefix, lookup_name }) catch {
            continue;
        };
        defer allocator.free(candidate);
        if (pricing_map.get(candidate)) |rate| {
            tryCachePricingAlias(allocator, pricing_map, alias_name, rate);
            return rate;
        }
    }
    return null;
}

fn lookupBySubstring(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?ModelPricing {
    var best_rate: ?ModelPricing = null;
    var best_score: usize = 0;

    var iterator = pricing_map.iterator();
    while (iterator.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.ascii.eqlIgnoreCase(key, lookup_name)) {
            tryCachePricingAlias(allocator, pricing_map, alias_name, entry.value_ptr.*);
            return entry.value_ptr.*;
        }

        const forward = std.ascii.indexOfIgnoreCase(key, lookup_name) != null;
        const backward = std.ascii.indexOfIgnoreCase(lookup_name, key) != null;
        if (!forward and !backward) continue;

        const score = if (forward)
            ratioScore(lookup_name.len, key.len)
        else
            ratioScore(key.len, lookup_name.len);

        if (score > best_score) {
            best_score = score;
            best_rate = entry.value_ptr.*;
        }
    }

    if (best_rate) |rate| {
        tryCachePricingAlias(allocator, pricing_map, alias_name, rate);
        return rate;
    }
    return null;
}

fn cachePricingAlias(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    alias: []const u8,
    pricing: ModelPricing,
) !void {
    if (pricing_map.get(alias) != null) return;
    const duplicate = try allocator.dupe(u8, alias);
    errdefer allocator.free(duplicate);
    try pricing_map.put(duplicate, pricing);
}

fn tryCachePricingAlias(
    allocator: std.mem.Allocator,
    pricing_map: *PricingMap,
    alias: []const u8,
    pricing: ModelPricing,
) void {
    cachePricingAlias(allocator, pricing_map, alias, pricing) catch |err| {
        std.log.warn("Failed to cache pricing alias for '{s}': {s}", .{ alias, @errorName(err) });
    };
}

fn ratioScore(numerator: usize, denominator: usize) usize {
    if (denominator == 0) return 0;
    const scaled = (@as(u128, numerator) * 100) / @as(u128, denominator);
    return std.math.cast(usize, scaled) orelse std.math.maxInt(usize);
}

fn createDateVariant(
    allocator: std.mem.Allocator,
    source: []const u8,
    from: u8,
    to: u8,
) !?[]u8 {
    var search_index: usize = 0;
    while (search_index < source.len) {
        const pos = std.mem.findScalarPos(u8, source, search_index, from) orelse return null;
        if (pos + 9 <= source.len and isEightDigitBlock(source[pos + 1 .. pos + 9])) {
            var copy = try allocator.dupe(u8, source);
            copy[pos] = to;
            return copy;
        }
        search_index = pos + 1;
    }
    return null;
}

fn isEightDigitBlock(block: []const u8) bool {
    if (block.len != 8) return false;
    for (block) |ch| {
        if (!std.ascii.isDigit(ch)) return false;
    }
    return true;
}

pub fn accumulateTotals(
    summaries: []const DailySummary,
    totals: *SummaryTotals,
) void {
    for (summaries) |summary| {
        totals.usage.add(summary.usage);
        totals.display_input_tokens += summary.display_input_tokens;
        totals.cost_usd += summary.cost_usd;
    }
}

pub fn collectMissingModels(
    allocator: std.mem.Allocator,
    missing_set: *std.StringHashMap(u8),
    output: *std.ArrayListUnmanaged([]const u8),
) !void {
    var iterator = missing_set.iterator();
    while (iterator.next()) |entry| {
        const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(owned_name);
        try output.append(allocator, owned_name);
    }
}

pub fn dateWithinFilters(filters: DateFilters, iso: []const u8) bool {
    if (filters.since) |since_value| {
        if (std.mem.lessThan(u8, iso, since_value[0..])) return false;
    }
    if (filters.until) |until_value| {
        if (std.mem.lessThan(u8, until_value[0..], iso)) return false;
    }
    return true;
}

fn updateSummary(
    summary: *DailySummary,
    allocator: std.mem.Allocator,
    event: *const TokenUsageEvent,
) !void {
    summary.usage.add(event.usage);
    summary.display_input_tokens += event.display_input_tokens;

    if (findModelIndex(summary, event.model)) |idx| {
        var existing = &summary.models.items[idx];
        existing.usage.add(event.usage);
        existing.display_input_tokens += event.display_input_tokens;
        if (event.is_fallback) existing.is_fallback = true;
        return;
    }

    const stored_name = try allocator.dupe(u8, event.model);
    try summary.models.append(allocator, .{
        .name = stored_name,
        .is_fallback = event.is_fallback,
        .pricing_available = false,
        .cost_usd = 0,
        .usage = event.usage,
        .display_input_tokens = event.display_input_tokens,
    });
}

fn findModelIndex(summary: *DailySummary, name: []const u8) ?usize {
    for (summary.models.items, 0..) |model, idx| {
        if (std.mem.eql(u8, model.name, name)) return idx;
    }
    return null;
}

fn rawUsageDelta(current: RawTokenUsage, previous_opt: ?RawTokenUsage) RawTokenUsage {
    const previous = previous_opt orelse RawTokenUsage{};
    return .{
        .input_tokens = subtractPositive(current.input_tokens, previous.input_tokens),
        .cache_creation_input_tokens = subtractPositive(
            current.cache_creation_input_tokens,
            previous.cache_creation_input_tokens,
        ),
        .cached_input_tokens = subtractPositive(current.cached_input_tokens, previous.cached_input_tokens),
        .output_tokens = subtractPositive(current.output_tokens, previous.output_tokens),
        .reasoning_output_tokens = subtractPositive(current.reasoning_output_tokens, previous.reasoning_output_tokens),
        .total_tokens = subtractPositive(current.total_tokens, previous.total_tokens),
    };
}

fn subtractPositive(current: u64, previous: u64) u64 {
    return if (current > previous) current - previous else 0;
}

fn appendUniqueString(
    allocator: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try list.append(allocator, value);
}

pub fn formatDisplayDate(allocator: std.mem.Allocator, iso_date: []const u8) ![]u8 {
    if (iso_date.len < 10) return error.InvalidDate;

    const year = try std.fmt.parseInt(u16, iso_date[0..4], 10);
    const month = try std.fmt.parseInt(u8, iso_date[5..7], 10);
    const day = try std.fmt.parseInt(u8, iso_date[8..10], 10);

    const months = [_][]const u8{
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    };
    if (month == 0 or month > months.len) return error.InvalidDate;

    return std.fmt.allocPrint(allocator, "{s} {d:0>2}, {d}", .{ months[month - 1], day, year });
}

test "resolveModelPricing handles anthropic prefixes" {
    const allocator = std.testing.allocator;
    var map = PricingMap.init(allocator);
    defer deinitPricingMap(&map, allocator);

    const key = try allocator.dupe(u8, "us.anthropic.claude-haiku-4-5-20251001-v1:0");
    try map.put(key, .{
        .input_cost_per_m = 1,
        .cache_creation_cost_per_m = 1,
        .cached_input_cost_per_m = 1,
        .output_cost_per_m = 1,
    });

    const rate = resolveModelPricing(allocator, &map, "claude-haiku-4-5-20251001") orelse unreachable;
    try std.testing.expectEqual(@as(f64, 1), rate.input_cost_per_m);
    try std.testing.expect(map.get("claude-haiku-4-5-20251001") != null);
}

test "resolveModelPricing falls back to substring match" {
    const allocator = std.testing.allocator;
    var map = PricingMap.init(allocator);
    defer deinitPricingMap(&map, allocator);

    const key = try allocator.dupe(u8, "openrouter/anthropic/claude-sonnet-4-5-20250929@20250929-v1:0");
    try map.put(key, .{
        .input_cost_per_m = 2,
        .cache_creation_cost_per_m = 2,
        .cached_input_cost_per_m = 2,
        .output_cost_per_m = 2,
    });

    const rate = resolveModelPricing(allocator, &map, "claude-sonnet-4-5-20250929") orelse unreachable;
    try std.testing.expectEqual(@as(f64, 2), rate.cache_creation_cost_per_m);
    try std.testing.expect(map.get("claude-sonnet-4-5-20250929") != null);
}

test "resolveModelPricing normalizes date separators" {
    const allocator = std.testing.allocator;
    var map = PricingMap.init(allocator);
    defer deinitPricingMap(&map, allocator);

    const key = try allocator.dupe(u8, "claude-3-opus@20240229");
    try map.put(key, .{
        .input_cost_per_m = 3,
        .cache_creation_cost_per_m = 3,
        .cached_input_cost_per_m = 3,
        .output_cost_per_m = 3,
    });

    const rate = resolveModelPricing(allocator, &map, "claude-3-opus-20240229") orelse unreachable;
    try std.testing.expectEqual(@as(f64, 3), rate.output_cost_per_m);
    try std.testing.expect(map.get("claude-3-opus-20240229") != null);
}
