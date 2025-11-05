const std = @import("std");

pub const MILLION = 1_000_000.0;
pub const PRICING_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

pub const DateFilters = struct {
    since: ?[10]u8 = null,
    until: ?[10]u8 = null,
    pretty_output: bool = false,
};

pub const ParseDateError = error{
    InvalidFormat,
    InvalidDate,
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

        const cache_creation_cost = (@as(f64, @floatFromInt(self.cache_creation_input_tokens)) / MILLION) * pricing.cache_creation_cost_per_m;
        const input_cost = (@as(f64, @floatFromInt(non_cached)) / MILLION) * pricing.input_cost_per_m;
        const cached_cost = (@as(f64, @floatFromInt(cached)) / MILLION) * pricing.cached_input_cost_per_m;
        const output_cost = (@as(f64, @floatFromInt(self.output_tokens)) / MILLION) * pricing.output_cost_per_m;

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

pub const TokenBuffer = struct {
    slice: []const u8,
    owned: ?[]u8 = null,

    pub fn release(self: *TokenBuffer, allocator: std.mem.Allocator) void {
        if (self.owned) |buf| allocator.free(buf);
        self.* = undefined;
    }
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
    if (std.mem.indexOfScalar(u8, slice, '.')) |_| {
        const parsed = std.fmt.parseFloat(f64, slice) catch return 0;
        return if (parsed >= 0)
            @as(u64, @intFromFloat(std.math.floor(parsed)))
        else
            0;
    }
    return std.fmt.parseInt(u64, slice, 10) catch 0;
}

pub const TokenUsageEvent = struct {
    session_id: []const u8,
    timestamp: []const u8,
    local_iso_date: [10]u8,
    model: []const u8,
    usage: TokenUsage,
    is_fallback: bool,
};

pub const ModelPricing = struct {
    input_cost_per_m: f64,
    cache_creation_cost_per_m: f64,
    cached_input_cost_per_m: f64,
    output_cost_per_m: f64,
};

pub const PricingMap = std.StringHashMap(ModelPricing);

pub const ModelSummary = struct {
    name: []const u8,
    is_fallback: bool,
    pricing_available: bool,
    cost_usd: f64,
    usage: TokenUsage,
};

pub const DailySummary = struct {
    iso_date: []const u8,
    display_date: []const u8,
    usage: TokenUsage,
    cost_usd: f64,
    models: std.ArrayListUnmanaged(ModelSummary),
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init(allocator: std.mem.Allocator, iso_date: []const u8, display_date: []const u8) DailySummary {
        _ = allocator;
        return .{
            .iso_date = iso_date,
            .display_date = display_date,
            .usage = .{},
            .cost_usd = 0,
            .models = .{},
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *DailySummary, allocator: std.mem.Allocator) void {
        self.models.deinit(allocator);
        self.missing_pricing.deinit(allocator);
    }
};

pub const SummaryTotals = struct {
    usage: TokenUsage = .{},
    cost_usd: f64 = 0,
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init() SummaryTotals {
        return .{
            .usage = .{},
            .cost_usd = 0,
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *SummaryTotals, allocator: std.mem.Allocator) void {
        self.missing_pricing.deinit(allocator);
    }
};

pub const SummaryBuilder = struct {
    summaries: std.ArrayListUnmanaged(DailySummary) = .{},
    date_index: std.StringHashMap(usize),
    event_count: usize = 0,

    pub fn init(allocator: std.mem.Allocator) SummaryBuilder {
        return .{
            .summaries = .{},
            .date_index = std.StringHashMap(usize).init(allocator),
            .event_count = 0,
        };
    }

    pub fn deinit(self: *SummaryBuilder, allocator: std.mem.Allocator) void {
        for (self.summaries.items) |*summary| {
            summary.deinit(allocator);
        }
        self.summaries.deinit(allocator);
        self.date_index.deinit();
    }

    pub fn ingest(
        self: *SummaryBuilder,
        allocator: std.mem.Allocator,
        arena: std.mem.Allocator,
        event: *const TokenUsageEvent,
        filters: DateFilters,
    ) !void {
        const iso_slice = event.local_iso_date[0..];
        if (!withinFilters(filters, iso_slice)) return;

        self.event_count += 1;

        if (self.date_index.get(iso_slice)) |idx| {
            try updateSummary(&self.summaries.items[idx], allocator, arena, event);
            return;
        }

        const iso_copy = try arena.dupe(u8, iso_slice);
        const display = try formatDisplayDate(arena, iso_copy);
        try self.summaries.append(allocator, DailySummary.init(allocator, iso_copy, display));
        const summary_idx = self.summaries.items.len - 1;
        try self.date_index.put(self.summaries.items[summary_idx].iso_date, summary_idx);
        try updateSummary(&self.summaries.items[summary_idx], allocator, arena, event);
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
        if (pricing_map.get(model.name)) |rate| {
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

pub fn accumulateTotals(
    allocator: std.mem.Allocator,
    summaries: []const DailySummary,
    totals: *SummaryTotals,
) void {
    for (summaries) |summary| {
        totals.usage.add(summary.usage);
        totals.cost_usd += summary.cost_usd;
        for (summary.missing_pricing.items) |name| {
            appendUniqueString(allocator, &totals.missing_pricing, name) catch {};
        }
    }
}

pub fn collectMissingModels(
    allocator: std.mem.Allocator,
    missing_set: *std.StringHashMap(u8),
    output: *std.ArrayListUnmanaged([]const u8),
) !void {
    var iterator = missing_set.iterator();
    while (iterator.next()) |entry| {
        try appendUniqueString(allocator, output, entry.key_ptr.*);
    }
}

fn withinFilters(filters: DateFilters, iso: []const u8) bool {
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
    arena: std.mem.Allocator,
    event: *const TokenUsageEvent,
) !void {
    summary.usage.add(event.usage);

    if (findModelIndex(summary, event.model)) |idx| {
        var existing = &summary.models.items[idx];
        existing.usage.add(event.usage);
        if (event.is_fallback) existing.is_fallback = true;
        return;
    }

    const stored_name = try arena.dupe(u8, event.model);
    try summary.models.append(allocator, .{
        .name = stored_name,
        .is_fallback = event.is_fallback,
        .pricing_available = false,
        .cost_usd = 0,
        .usage = event.usage,
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

fn formatDisplayDate(arena: std.mem.Allocator, iso_date: []const u8) ![]u8 {
    if (iso_date.len < 10) return error.InvalidDate;

    const year = try std.fmt.parseInt(u16, iso_date[0..4], 10);
    const month = try std.fmt.parseInt(u8, iso_date[5..7], 10);
    const day = try std.fmt.parseInt(u8, iso_date[8..10], 10);

    const months = [_][]const u8{
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    };
    if (month == 0 or month > months.len) return error.InvalidDate;

    return std.fmt.allocPrint(arena, "{s} {d:0>2}, {d}", .{ months[month - 1], day, year });
}
