const std = @import("std");
const Model = @import("model.zig");
const codex = @import("providers/codex.zig");

pub const DateFilters = struct {
    since: ?[10]u8 = null,
    until: ?[10]u8 = null,
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

const ModelSummary = struct {
    name: []const u8,
    is_fallback: bool,
    pricing_available: bool,
    cost_usd: f64,
    usage: Model.TokenUsage,
};

const DailySummary = struct {
    iso_date: []const u8,
    display_date: []const u8,
    usage: Model.TokenUsage,
    cost_usd: f64,
    models: std.ArrayListUnmanaged(ModelSummary),
    model_index: std.StringHashMap(usize),
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    fn init(allocator: std.mem.Allocator, iso_date: []const u8, display_date: []const u8) DailySummary {
        return .{
            .iso_date = iso_date,
            .display_date = display_date,
            .usage = .{},
            .cost_usd = 0,
            .models = .{},
            .model_index = std.StringHashMap(usize).init(allocator),
            .missing_pricing = .{},
        };
    }

    fn deinit(self: *DailySummary, allocator: std.mem.Allocator) void {
        self.models.deinit(allocator);
        self.model_index.deinit();
        self.missing_pricing.deinit(allocator);
    }
};

const SummaryTotals = struct {
    usage: Model.TokenUsage = .{},
    cost_usd: f64 = 0,
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    fn init() SummaryTotals {
        return .{
            .usage = .{},
            .cost_usd = 0,
            .missing_pricing = .{},
        };
    }

    fn deinit(self: *SummaryTotals, allocator: std.mem.Allocator) void {
        self.missing_pricing.deinit(allocator);
    }
};

pub fn run(allocator: std.mem.Allocator, filters: DateFilters) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var events = std.ArrayListUnmanaged(Model.TokenUsageEvent){};
    defer events.deinit(allocator);

    var pricing_map = Model.PricingMap.init(allocator);
    defer pricing_map.deinit();

    try codex.collect(allocator, arena, &events, &pricing_map);

    var out_writer = OutputWriter{ .file = std.fs.File.stdout() };

    if (events.items.len == 0) {
        try out_writer.writeAll("{\"days\":[],\"total\":{\"input_tokens\":0,\"cached_input_tokens\":0,\"output_tokens\":0,\"reasoning_output_tokens\":0,\"total_tokens\":0,\"cost_usd\":0.0,\"missing_pricing\":[]}}\n");
        return;
    }

    std.sort.pdq(Model.TokenUsageEvent, events.items, {}, eventLessThan);

    var summaries = std.ArrayListUnmanaged(DailySummary){};
    defer {
        for (summaries.items) |*summary| {
            summary.deinit(allocator);
        }
        summaries.deinit(allocator);
    }

    var date_index = std.StringHashMap(usize).init(allocator);
    defer date_index.deinit();

    try buildDailySummaries(allocator, arena, events.items, &summaries, &date_index, filters);

    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();

    for (summaries.items) |*summary| {
        applyPricing(allocator, summary, &pricing_map, &missing_set);
        std.sort.pdq(ModelSummary, summary.models.items, {}, modelLessThan);
    }

    std.sort.pdq(DailySummary, summaries.items, {}, summaryLessThan);

    var totals = SummaryTotals.init();
    defer totals.deinit(allocator);
    accumulateTotals(allocator, &summaries, &totals);
    try collectMissingModels(allocator, &missing_set, &totals.missing_pricing);

    try writeJson(&out_writer, summaries.items, &totals);
}

fn buildDailySummaries(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: []const Model.TokenUsageEvent,
    summaries: *std.ArrayListUnmanaged(DailySummary),
    date_index: *std.StringHashMap(usize),
    filters: DateFilters,
) !void {
    for (events) |event| {
        const iso_slice = event.local_iso_date[0..];
        if (!withinFilters(filters, iso_slice)) continue;

        if (date_index.get(iso_slice)) |idx| {
            try updateSummary(&summaries.items[idx], allocator, arena, &event);
            continue;
        }

        const iso_copy = try arena.dupe(u8, iso_slice);
        const display = try formatDisplayDate(arena, iso_copy);
        try summaries.append(allocator, DailySummary.init(allocator, iso_copy, display));
        const summary_idx = summaries.items.len - 1;
        try date_index.put(iso_copy, summary_idx);
        try updateSummary(&summaries.items[summary_idx], allocator, arena, &event);
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
    event: *const Model.TokenUsageEvent,
) !void {
    summary.usage.add(event.usage);

    if (summary.model_index.get(event.model)) |idx| {
        var model_summary = &summary.models.items[idx];
        model_summary.usage.add(event.usage);
        if (event.is_fallback) model_summary.is_fallback = true;
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
    const new_index = summary.models.items.len - 1;
    try summary.model_index.put(summary.models.items[new_index].name, new_index);
}

fn applyPricing(
    allocator: std.mem.Allocator,
    summary: *DailySummary,
    pricing_map: *Model.PricingMap,
    missing_set: *std.StringHashMap(u8),
) void {
    summary.cost_usd = 0;
    summary.missing_pricing.clearRetainingCapacity();

    for (summary.models.items) |*model| {
        if (pricing_map.get(model.name)) |rate| {
            model.pricing_available = true;
            model.cost_usd = calculateCost(model.usage, rate);
            summary.cost_usd += model.cost_usd;
        } else {
            model.pricing_available = false;
            model.cost_usd = 0;
            _ = missing_set.put(model.name, 1) catch {};
            appendUniqueString(allocator, &summary.missing_pricing, model.name) catch {};
        }
    }
}

fn calculateCost(usage: Model.TokenUsage, pricing: Model.ModelPricing) f64 {
    const non_cached = if (usage.input_tokens > usage.cached_input_tokens)
        usage.input_tokens - usage.cached_input_tokens
    else
        0;
    const cached = if (usage.cached_input_tokens > usage.input_tokens)
        usage.input_tokens
    else
        usage.cached_input_tokens;

    const input_cost = (@as(f64, @floatFromInt(non_cached)) / Model.MILLION) * pricing.input_cost_per_m;
    const cached_cost = (@as(f64, @floatFromInt(cached)) / Model.MILLION) * pricing.cached_input_cost_per_m;
    const output_cost = (@as(f64, @floatFromInt(usage.output_tokens)) / Model.MILLION) * pricing.output_cost_per_m;

    return input_cost + cached_cost + output_cost;
}

fn accumulateTotals(
    allocator: std.mem.Allocator,
    summaries: *std.ArrayListUnmanaged(DailySummary),
    totals: *SummaryTotals,
) void {
    for (summaries.items) |summary| {
        totals.usage.add(summary.usage);
        totals.cost_usd += summary.cost_usd;
        for (summary.missing_pricing.items) |name| {
            appendUniqueString(allocator, &totals.missing_pricing, name) catch {};
        }
    }
}

fn collectMissingModels(
    allocator: std.mem.Allocator,
    missing_set: *std.StringHashMap(u8),
    output: *std.ArrayListUnmanaged([]const u8),
) !void {
    var iterator = missing_set.iterator();
    while (iterator.next()) |entry| {
        try appendUniqueString(allocator, output, entry.key_ptr.*);
    }
}

fn writeJson(
    writer: anytype,
    summaries: []DailySummary,
    totals: *const SummaryTotals,
) !void {
    try writer.writeAll("{\"days\":[");
    for (summaries, 0..) |summary, index| {
        if (index != 0) try writer.writeAll(",");
        try writeSummaryJson(writer, summary);
    }
    try writer.writeAll("],\"total\":");
    try writeTotalsJson(writer, totals);
    try writer.writeAll("}\n");
}

fn writeSummaryJson(writer: anytype, summary: DailySummary) !void {
    try writer.writeAll("{\"date\":");
    try writeJsonString(writer, summary.display_date);
    try writer.writeAll(",\"iso_date\":");
    try writeJsonString(writer, summary.iso_date);
    try writer.writeAll(",\"input_tokens\":");
    try writeUint(writer, summary.usage.input_tokens);
    try writer.writeAll(",\"cached_input_tokens\":");
    try writeUint(writer, summary.usage.cached_input_tokens);
    try writer.writeAll(",\"output_tokens\":");
    try writeUint(writer, summary.usage.output_tokens);
    try writer.writeAll(",\"reasoning_output_tokens\":");
    try writeUint(writer, summary.usage.reasoning_output_tokens);
    try writer.writeAll(",\"total_tokens\":");
    try writeUint(writer, summary.usage.total_tokens);
    try writer.writeAll(",\"cost_usd\":");
    try writeFloat(writer, summary.cost_usd);
    try writer.writeAll(",\"models\":[");
    for (summary.models.items, 0..) |model, idx| {
        if (idx != 0) try writer.writeAll(",");
        try writeModelJson(writer, model);
    }
    try writer.writeAll("],\"missing_pricing\":[");
    for (summary.missing_pricing.items, 0..) |name, idx| {
        if (idx != 0) try writer.writeAll(",");
        try writeJsonString(writer, name);
    }
    try writer.writeAll("]}");
}

fn writeModelJson(writer: anytype, model: ModelSummary) !void {
    try writer.writeAll("{\"name\":");
    try writeJsonString(writer, model.name);
    try writer.writeAll(",\"display_name\":");
    if (model.is_fallback) {
        var buffer: [128]u8 = undefined;
        const display = std.fmt.bufPrint(&buffer, "{s} (fallback)", .{model.name}) catch model.name;
        try writeJsonString(writer, display);
    } else {
        try writeJsonString(writer, model.name);
    }
    try writer.writeAll(",\"is_fallback\":");
    try writer.writeAll(if (model.is_fallback) "true" else "false");
    try writer.writeAll(",\"input_tokens\":");
    try writeUint(writer, model.usage.input_tokens);
    try writer.writeAll(",\"cached_input_tokens\":");
    try writeUint(writer, model.usage.cached_input_tokens);
    try writer.writeAll(",\"output_tokens\":");
    try writeUint(writer, model.usage.output_tokens);
    try writer.writeAll(",\"reasoning_output_tokens\":");
    try writeUint(writer, model.usage.reasoning_output_tokens);
    try writer.writeAll(",\"total_tokens\":");
    try writeUint(writer, model.usage.total_tokens);
    try writer.writeAll(",\"cost_usd\":");
    try writeFloat(writer, model.cost_usd);
    try writer.writeAll(",\"pricing_available\":");
    try writer.writeAll(if (model.pricing_available) "true" else "false");
    try writer.writeAll("}");
}

fn writeTotalsJson(writer: anytype, totals: *const SummaryTotals) !void {
    try writer.writeAll("{\"input_tokens\":");
    try writeUint(writer, totals.usage.input_tokens);
    try writer.writeAll(",\"cached_input_tokens\":");
    try writeUint(writer, totals.usage.cached_input_tokens);
    try writer.writeAll(",\"output_tokens\":");
    try writeUint(writer, totals.usage.output_tokens);
    try writer.writeAll(",\"reasoning_output_tokens\":");
    try writeUint(writer, totals.usage.reasoning_output_tokens);
    try writer.writeAll(",\"total_tokens\":");
    try writeUint(writer, totals.usage.total_tokens);
    try writer.writeAll(",\"cost_usd\":");
    try writeFloat(writer, totals.cost_usd);
    try writer.writeAll(",\"missing_pricing\":[");
    for (totals.missing_pricing.items, 0..) |name, idx| {
        if (idx != 0) try writer.writeAll(",");
        try writeJsonString(writer, name);
    }
    try writer.writeAll("]}");
}

fn writeFloat(writer: anytype, value: f64) !void {
    if (value == 0) {
        try writer.writeAll("0.0");
        return;
    }
    var buffer: [64]u8 = undefined;
    const text = try std.fmt.bufPrint(&buffer, "{d:.2}", .{value});
    try writer.writeAll(text);
}

fn writeUint(writer: anytype, value: u64) !void {
    var buffer: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buffer, "{d}", .{value});
    try writer.writeAll(text);
}

const OutputWriter = struct {
    file: std.fs.File,

    fn writeAll(self: *OutputWriter, bytes: []const u8) !void {
        try self.file.writeAll(bytes);
    }
};

fn writeJsonString(writer: anytype, value: []const u8) !void {
    try writer.writeAll("\"");
    for (value) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (ch < 0x20) {
                    var buf: [6]u8 = undefined;
                    const formatted = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{ch}) catch unreachable;
                    try writer.writeAll(formatted);
                } else {
                    try writer.writeAll(&[_]u8{ch});
                }
            },
        }
    }
    try writer.writeAll("\"");
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

fn eventLessThan(_: void, lhs: Model.TokenUsageEvent, rhs: Model.TokenUsageEvent) bool {
    if (std.mem.eql(u8, lhs.timestamp, rhs.timestamp)) {
        if (!std.mem.eql(u8, lhs.session_id, rhs.session_id)) {
            return std.mem.lessThan(u8, lhs.session_id, rhs.session_id);
        }
        return std.mem.lessThan(u8, lhs.model, rhs.model);
    }
    return std.mem.lessThan(u8, lhs.timestamp, rhs.timestamp);
}

fn summaryLessThan(_: void, lhs: DailySummary, rhs: DailySummary) bool {
    return std.mem.lessThan(u8, lhs.iso_date, rhs.iso_date);
}

fn modelLessThan(_: void, lhs: ModelSummary, rhs: ModelSummary) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}
