const std = @import("std");
const model = @import("model.zig");

pub fn applyPricing(
    allocator: std.mem.Allocator,
    summary: *model.DailySummary,
    pricing_map: *model.PricingMap,
    missing_set: *std.StringHashMap(u8),
) void {
    summary.cost_usd = 0;
    summary.missing_pricing.clearRetainingCapacity();

    for (summary.models.items) |*mod| {
        if (resolveModelPricing(allocator, pricing_map, mod.name)) |rate| {
            mod.pricing_available = true;
            mod.cost_usd = mod.usage.cost(rate);
            summary.cost_usd += mod.cost_usd;
        } else {
            mod.pricing_available = false;
            mod.cost_usd = 0;
            _ = missing_set.put(mod.name, 1) catch {};
            appendUniqueString(allocator, &summary.missing_pricing, mod.name) catch {};
        }
    }
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

pub fn applySessionRecorderPricing(
    allocator: std.mem.Allocator,
    recorder: *model.SessionRecorder,
    pricing_map: *model.PricingMap,
) void {
    recorder.total_cost_usd = 0;
    var iterator = recorder.sessions.iterator();
    while (iterator.next()) |entry| {
        var session = entry.value_ptr;
        applySessionEntryPricing(allocator, session, pricing_map);
        recorder.total_cost_usd += session.cost_usd;
    }
}

fn applySessionEntryPricing(
    allocator: std.mem.Allocator,
    session: *model.SessionRecorder.SessionEntry,
    pricing_map: *model.PricingMap,
) void {
    session.cost_usd = 0;
    for (session.models.items) |*mod| {
        if (resolveModelPricing(allocator, pricing_map, mod.name)) |rate| {
            mod.pricing_available = true;
            mod.cost_usd = mod.usage.cost(rate);
            session.cost_usd += mod.cost_usd;
        } else {
            mod.pricing_available = false;
            mod.cost_usd = 0;
        }
    }
    std.sort.pdq(model.SessionRecorder.SessionModel, session.models.items, {}, sessionModelLessThan);
}

fn sessionModelLessThan(_: void, lhs: model.SessionRecorder.SessionModel, rhs: model.SessionRecorder.SessionModel) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

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

fn resolveModelPricing(
    allocator: std.mem.Allocator,
    pricing_map: *model.PricingMap,
    model_name: []const u8,
) ?model.ModelPricing {
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
    pricing_map: *model.PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?model.ModelPricing {
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
    pricing_map: *model.PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?model.ModelPricing {
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
    pricing_map: *model.PricingMap,
    lookup_name: []const u8,
    alias_name: []const u8,
) ?model.ModelPricing {
    var best_rate: ?model.ModelPricing = null;
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
    pricing_map: *model.PricingMap,
    alias: []const u8,
    pricing: model.ModelPricing,
) !void {
    if (pricing_map.get(alias) != null) return;
    const duplicate = try allocator.dupe(u8, alias);
    errdefer allocator.free(duplicate);
    try pricing_map.put(duplicate, pricing);
}

fn tryCachePricingAlias(
    allocator: std.mem.Allocator,
    pricing_map: *model.PricingMap,
    alias: []const u8,
    pricing: model.ModelPricing,
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

test "resolveModelPricing handles anthropic prefixes" {
    const allocator = std.testing.allocator;
    var map = model.PricingMap.init(allocator);
    defer model.deinitPricingMap(&map, allocator);

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
    var map = model.PricingMap.init(allocator);
    defer model.deinitPricingMap(&map, allocator);

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
    var map = model.PricingMap.init(allocator);
    defer model.deinitPricingMap(&map, allocator);

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
