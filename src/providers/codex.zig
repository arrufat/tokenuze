const std = @import("std");
const Model = @import("../model.zig");
const timeutil = @import("../time.zig");

const PRICING_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
const LEGACY_FALLBACK_MODEL = "gpt-5";
const JSON_EXT = ".jsonl";

const FALLBACK_PRICING = [_]struct {
    name: []const u8,
    pricing: Model.ModelPricing,
}{
    .{ .name = "gpt-5", .pricing = .{ .input_cost_per_m = 1.25, .cached_input_cost_per_m = 0.125, .output_cost_per_m = 10.0 } },
    .{ .name = "gpt-5-codex", .pricing = .{ .input_cost_per_m = 1.25, .cached_input_cost_per_m = 0.125, .output_cost_per_m = 10.0 } },
};

const RawUsage = struct {
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_output_tokens: u64,
    total_tokens: u64,
};

pub fn collect(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
    pricing: *Model.PricingMap,
) !void {
    try collectEvents(allocator, arena, events);
    try loadPricing(arena, allocator, pricing);
}

fn collectEvents(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
) !void {
    const sessions_dir = resolveSessionsDir(allocator) catch return;
    defer allocator.free(sessions_dir);

    var root_dir = std.fs.openDirAbsolute(sessions_dir, .{ .iterate = true }) catch return;
    defer root_dir.close();

    var walker = try root_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        const relative_path = std.mem.sliceTo(entry.path, 0);
        if (!std.mem.endsWith(u8, relative_path, JSON_EXT)) continue;

        const session_id_slice = relative_path[0 .. relative_path.len - JSON_EXT.len];
        const session_id = try arena.dupe(u8, session_id_slice);

        const absolute_path = try std.fs.path.join(allocator, &.{ sessions_dir, relative_path });
        defer allocator.free(absolute_path);

        try parseSessionFile(allocator, arena, session_id, absolute_path, events);
    }
}

fn resolveSessionsDir(allocator: std.mem.Allocator) ![]u8 {
    const home = try std.process.getEnvVarOwned(allocator, "HOME");
    defer allocator.free(home);
    return std.fmt.allocPrint(allocator, "{s}/.codex/sessions", .{home});
}

fn parseSessionFile(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    session_id: []const u8,
    file_path: []const u8,
    events: *std.ArrayListUnmanaged(Model.TokenUsageEvent),
) !void {
    var previous_totals: ?RawUsage = null;
    var current_model: ?[]const u8 = null;
    var current_model_is_fallback = false;
    const max_session_size: usize = 128 * 1024 * 1024;
    const file_bytes = std.fs.cwd().readFileAlloc(file_path, allocator, std.Io.Limit.limited(max_session_size)) catch {
        return;
    };
    defer allocator.free(file_bytes);

    var lines = std.mem.splitScalar(u8, file_bytes, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0) continue;

        var parsed = std.json.parseFromSlice(std.json.Value, allocator, trimmed, .{ .ignore_unknown_fields = true }) catch {
            continue;
        };
        defer parsed.deinit();

        const root = parsed.value;
        const entry_type = objectGetString(root, "type") orelse continue;

        if (std.mem.eql(u8, entry_type, "turn_context")) {
            const payload = objectGet(root, "payload");
            if (payload) |payload_value| {
                if (extractModel(arena, payload_value)) |model_name| {
                    current_model = model_name;
                    current_model_is_fallback = false;
                }
            }
            continue;
        }

        if (!std.mem.eql(u8, entry_type, "event_msg")) continue;

        const payload = objectGet(root, "payload") orelse continue;
        const payload_type = objectGetString(payload, "type") orelse continue;
        if (!std.mem.eql(u8, payload_type, "token_count")) continue;

        const timestamp_str = objectGetString(root, "timestamp") orelse continue;
        const timestamp = try duplicateNonEmpty(arena, timestamp_str) orelse continue;
        const iso_date = timeutil.localIsoDateFromTimestamp(timestamp) catch continue;

        const info = objectGet(payload, "info");
        var raw: ?RawUsage = null;
        if (info) |info_value| {
            raw = normalizeRawUsage(objectGet(info_value, "last_token_usage"));
            const total_usage = normalizeRawUsage(objectGet(info_value, "total_token_usage"));
            if (raw == null and total_usage != null) {
                raw = subtractRawUsage(total_usage.?, previous_totals);
            }
            if (total_usage != null) {
                previous_totals = total_usage;
            }
        }

        if (raw == null) continue;
        const delta = convertToDelta(raw.?);
        if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
            continue;
        }

        const extracted_model = extractModel(arena, payload);
        if (extracted_model) |name| {
            current_model = name;
            current_model_is_fallback = false;
        }

        var model_name = extracted_model;
        var is_fallback = false;
        if (model_name == null) {
            if (current_model) |known| {
                model_name = known;
                is_fallback = current_model_is_fallback;
            } else {
                model_name = LEGACY_FALLBACK_MODEL;
                is_fallback = true;
                current_model = model_name;
                current_model_is_fallback = true;
            }
        } else {
            current_model = model_name;
            current_model_is_fallback = false;
        }

        if (model_name == null) continue;
        if (std.mem.eql(u8, model_name.?, LEGACY_FALLBACK_MODEL) and extracted_model == null) {
            is_fallback = true;
            current_model_is_fallback = true;
        }

        const event = Model.TokenUsageEvent{
            .session_id = session_id,
            .timestamp = timestamp,
            .local_iso_date = iso_date,
            .model = model_name.?,
            .usage = delta,
            .is_fallback = is_fallback,
        };
        try events.append(allocator, event);
    }
}

fn normalizeRawUsage(value_opt: ?std.json.Value) ?RawUsage {
    const value = value_opt orelse return null;
    return switch (value) {
        .object => |obj| {
            const input = ensureNumber(obj.get("input_tokens"));
            const cached_direct = ensureNumber(obj.get("cached_input_tokens"));
            const cached_fallback = ensureNumber(obj.get("cache_read_input_tokens"));
            const cached = if (cached_direct > 0) cached_direct else cached_fallback;

            return RawUsage{
                .input_tokens = input,
                .cached_input_tokens = cached,
                .output_tokens = ensureNumber(obj.get("output_tokens")),
                .reasoning_output_tokens = ensureNumber(obj.get("reasoning_output_tokens")),
                .total_tokens = ensureNumber(obj.get("total_tokens")),
            };
        },
        else => null,
    };
}

fn ensureNumber(node_opt: ?std.json.Value) u64 {
    const node = node_opt orelse return 0;
    return switch (node) {
        .integer => |value| if (value >= 0) @as(u64, @intCast(value)) else 0,
        .float => |value| if (value >= 0) @as(u64, @intFromFloat(std.math.floor(value))) else 0,
        else => 0,
    };
}

fn subtractRawUsage(current: RawUsage, previous_opt: ?RawUsage) RawUsage {
    const previous = previous_opt orelse RawUsage{
        .input_tokens = 0,
        .cached_input_tokens = 0,
        .output_tokens = 0,
        .reasoning_output_tokens = 0,
        .total_tokens = 0,
    };

    return .{
        .input_tokens = subtractPositive(current.input_tokens, previous.input_tokens),
        .cached_input_tokens = subtractPositive(current.cached_input_tokens, previous.cached_input_tokens),
        .output_tokens = subtractPositive(current.output_tokens, previous.output_tokens),
        .reasoning_output_tokens = subtractPositive(current.reasoning_output_tokens, previous.reasoning_output_tokens),
        .total_tokens = subtractPositive(current.total_tokens, previous.total_tokens),
    };
}

fn subtractPositive(current: u64, previous: u64) u64 {
    return if (current > previous) current - previous else 0;
}

fn convertToDelta(raw: RawUsage) Model.TokenUsage {
    const total = if (raw.total_tokens > 0) raw.total_tokens else raw.input_tokens + raw.output_tokens;
    const cached = if (raw.cached_input_tokens > raw.input_tokens) raw.input_tokens else raw.cached_input_tokens;

    return .{
        .input_tokens = raw.input_tokens,
        .cached_input_tokens = cached,
        .output_tokens = raw.output_tokens,
        .reasoning_output_tokens = raw.reasoning_output_tokens,
        .total_tokens = total,
    };
}

fn loadPricing(
    arena: std.mem.Allocator,
    allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !void {
    fetchRemotePricing(arena, allocator, pricing) catch {};

    if (pricing.count() == 0) {
        try addFallbackPricing(arena, pricing);
    } else {
        try ensureFallbackPricing(arena, pricing);
    }
}

fn fetchRemotePricing(
    arena: std.mem.Allocator,
    allocator: std.mem.Allocator,
    pricing: *Model.PricingMap,
) !void {
    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer_ctx = CollectWriter.init(&buffer, allocator);

    const result = try client.fetch(.{
        .location = .{ .url = PRICING_URL },
        .response_writer = &writer_ctx.base,
    });

    if (result.status.class() != .success) return error.HttpError;

    const payload = try arena.dupe(u8, buffer.items);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidResponse;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        try addPricingFromJson(arena, pricing, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn addPricingFromJson(
    arena: std.mem.Allocator,
    pricing: *Model.PricingMap,
    model_name: []const u8,
    value: std.json.Value,
) !void {
    if (pricing.get(model_name) != null) return;
    if (value != .object) return;

    const obj = value.object;
    const input = getNumericField(obj, "input_cost_per_token") orelse return;
    const output = getNumericField(obj, "output_cost_per_token") orelse return;
    const cached = getNumericField(obj, "cache_read_input_token_cost") orelse input;

    const key = try arena.dupe(u8, model_name);
    try pricing.put(key, .{
        .input_cost_per_m = input * Model.MILLION,
        .cached_input_cost_per_m = cached * Model.MILLION,
        .output_cost_per_m = output * Model.MILLION,
    });
}

fn addFallbackPricing(arena: std.mem.Allocator, pricing: *Model.PricingMap) !void {
    for (FALLBACK_PRICING) |fallback| {
        if (pricing.get(fallback.name) != null) continue;
        const key = try arena.dupe(u8, fallback.name);
        try pricing.put(key, fallback.pricing);
    }
}

fn ensureFallbackPricing(arena: std.mem.Allocator, pricing: *Model.PricingMap) !void {
    for (FALLBACK_PRICING) |fallback| {
        if (pricing.get(fallback.name) != null) continue;
        const key = try arena.dupe(u8, fallback.name);
        try pricing.put(key, fallback.pricing);
    }
}

fn getNumericField(
    object: std.json.ObjectMap,
    field: []const u8,
) ?f64 {
    const entry = object.get(field) orelse return null;
    return valueAsFloat(entry);
}

fn valueAsFloat(value: std.json.Value) ?f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        .number_string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}

fn objectGet(value: std.json.Value, key: []const u8) ?std.json.Value {
    return switch (value) {
        .object => |obj| obj.get(key),
        else => null,
    };
}

fn objectGetString(value: std.json.Value, key: []const u8) ?[]const u8 {
    return switch (value) {
        .object => |obj| {
            const entry = obj.get(key) orelse return null;
            return switch (entry) {
                .string => |text| text,
                else => null,
            };
        },
        else => null,
    };
}

fn duplicateNonEmpty(arena: std.mem.Allocator, value: []const u8) !?[]const u8 {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return try arena.dupe(u8, trimmed);
}

fn extractModel(arena: std.mem.Allocator, payload: std.json.Value) ?[]const u8 {
    if (payload != .object) return null;

    if (objectGet(payload, "info")) |info_obj| {
        if (findModelCandidate(arena, info_obj)) |model| {
            return model;
        }
        if (objectGet(info_obj, "metadata")) |meta| {
            if (findModelCandidate(arena, meta)) |model| {
                return model;
            }
        }
    }

    if (findModelCandidate(arena, payload)) |model| {
        return model;
    }

    if (objectGet(payload, "metadata")) |metadata_obj| {
        if (findModelCandidate(arena, metadata_obj)) |model| {
            return model;
        }
    }

    return null;
}

fn findModelCandidate(arena: std.mem.Allocator, value: std.json.Value) ?[]const u8 {
    if (value != .object) return null;
    const candidates = [_][]const u8{ "model", "model_name" };
    for (candidates) |key| {
        if (objectGetString(value, key)) |candidate| {
            const maybe = duplicateNonEmpty(arena, candidate) catch null;
            if (maybe) |dup| return dup;
        }
    }
    return null;
}

const CollectWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,

    fn init(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator) CollectWriter {
        return .{
            .base = .{
                .vtable = &.{
                    .drain = CollectWriter.drain,
                    .sendFile = std.Io.Writer.unimplementedSendFile,
                    .flush = CollectWriter.flush,
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
        const self: *CollectWriter = @fieldParentPtr("base", writer);
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
