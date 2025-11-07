const std = @import("std");
const model = @import("../model.zig");
const timeutil = @import("../time.zig");
const SessionProvider = @import("session_provider.zig");

const RawUsage = model.RawTokenUsage;
const ModelState = SessionProvider.ModelState;

const GEMINI_USAGE_FIELDS = [_]SessionProvider.UsageFieldDescriptor{
    .{ .key = "input", .field = .input_tokens },
    .{ .key = "cached", .field = .cached_input_tokens },
    .{ .key = "output", .field = .output_tokens },
    .{ .key = "tool", .field = .output_tokens, .mode = .add },
    .{ .key = "thoughts", .field = .reasoning_output_tokens },
    .{ .key = "total", .field = .total_tokens },
};

const fallback_pricing = [_]SessionProvider.FallbackPricingEntry{
    .{ .name = "gemini-2.5-pro", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
    .{ .name = "gemini-flash-latest", .pricing = .{
        .input_cost_per_m = 0.30,
        .cache_creation_cost_per_m = 0.30,
        .cached_input_cost_per_m = 0.075,
        .output_cost_per_m = 2.50,
    } },
    .{ .name = "gemini-1.5-pro", .pricing = .{
        .input_cost_per_m = 3.50,
        .cache_creation_cost_per_m = 3.50,
        .cached_input_cost_per_m = 3.50,
        .output_cost_per_m = 10.50,
    } },
    .{ .name = "gemini-1.5-flash", .pricing = .{
        .input_cost_per_m = 0.35,
        .cache_creation_cost_per_m = 0.35,
        .cached_input_cost_per_m = 0.35,
        .output_cost_per_m = 1.05,
    } },
};

const ProviderExports = SessionProvider.makeProvider(.{
    .name = "gemini",
    .sessions_dir_suffix = "/.gemini/tmp",
    .legacy_fallback_model = null,
    .fallback_pricing = fallback_pricing[0..],
    .session_file_ext = ".json",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseSessionFile,
});

pub const collect = ProviderExports.collect;
pub const loadPricingData = ProviderExports.loadPricingData;

fn parseSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const SessionProvider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*SessionProvider.MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    _ = deduper;
    try parseGeminiSessionFile(allocator, ctx, session_id, file_path, timezone_offset_minutes, events);
}

fn parseGeminiSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const SessionProvider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    const max_session_size: usize = 32 * 1024 * 1024;
    const file_data = std.fs.cwd().readFileAlloc(file_path, allocator, std.Io.Limit.limited(max_session_size)) catch |err| {
        ctx.logWarning(file_path, "failed to read gemini session file", err);
        return;
    };
    defer allocator.free(file_data);

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, file_data, .{}) catch |err| {
        ctx.logWarning(file_path, "failed to parse gemini session file", err);
        return;
    };
    defer parsed.deinit();

    const root_value = parsed.value;
    const session_obj = switch (root_value) {
        .object => |obj| obj,
        else => return,
    };

    var session_label = session_id;
    if (session_obj.get("sessionId")) |sid_value| {
        switch (sid_value) {
            .string => |slice| {
                if (try SessionProvider.duplicateNonEmpty(allocator, slice)) |dup| {
                    session_label = dup;
                }
            },
            else => {},
        }
    }

    const messages_value = session_obj.get("messages") orelse return;
    const messages = switch (messages_value) {
        .array => |arr| arr.items,
        else => return,
    };
    if (messages.len == 0) return;

    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    for (messages) |message_value| {
        switch (message_value) {
            .object => |msg_obj| {
                const tokens_value = msg_obj.get("tokens") orelse continue;
                const tokens_obj = switch (tokens_value) {
                    .object => |obj| obj,
                    else => continue,
                };

                const timestamp_value = msg_obj.get("timestamp") orelse continue;
                const timestamp_slice = switch (timestamp_value) {
                    .string => |slice| slice,
                    else => continue,
                };
                const timestamp_copy = try SessionProvider.duplicateNonEmpty(allocator, timestamp_slice) orelse continue;
                const iso_date = timeutil.isoDateForTimezone(timestamp_copy, timezone_offset_minutes) catch {
                    continue;
                };

                const message_model = msg_obj.get("model");
                _ = ctx.captureModel(allocator, &model_state, message_model) catch false;

                const current_raw = parseGeminiUsage(tokens_obj);
                var delta = model.TokenUsage.deltaFrom(current_raw, previous_totals);
                ctx.normalizeUsageDelta(&delta);
                previous_totals = current_raw;

                if (delta.input_tokens == 0 and delta.cached_input_tokens == 0 and delta.output_tokens == 0 and delta.reasoning_output_tokens == 0) {
                    continue;
                }

                const resolved_model = (try ctx.requireModel(allocator, &model_state, null)) orelse continue;

                const event = model.TokenUsageEvent{
                    .session_id = session_label,
                    .timestamp = timestamp_copy,
                    .local_iso_date = iso_date,
                    .model = resolved_model.name,
                    .usage = delta,
                    .is_fallback = resolved_model.is_fallback,
                    .display_input_tokens = ctx.computeDisplayInput(delta),
                };
                try events.append(allocator, event);
            },
            else => continue,
        }
    }
}

fn parseGeminiUsage(tokens_obj: std.json.ObjectMap) RawUsage {
    return SessionProvider.parseUsageObject(tokens_obj, GEMINI_USAGE_FIELDS[0..]);
}

test "gemini parser converts message totals into usage deltas" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);

    const ctx = SessionProvider.ParseContext{
        .provider_name = "gemini-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    try parseGeminiSessionFile(
        worker_allocator,
        &ctx,
        "gemini-fixture",
        "fixtures/gemini/basic.json",
        0,
        &events,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("gem-session", event.session_id);
    try std.testing.expectEqualStrings("gemini-1.5-pro", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 4000), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 500), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 125), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 20), event.usage.reasoning_output_tokens);
    try std.testing.expectEqual(@as(u64, 4000), event.display_input_tokens);
}
