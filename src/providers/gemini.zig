const std = @import("std");

const model = @import("../model.zig");
const RawUsage = model.RawTokenUsage;
const provider = @import("provider.zig");
const ModelState = provider.ModelState;

const fallback_pricing = [_]provider.FallbackPricingEntry{
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

const ProviderExports = provider.makeProvider(.{
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
pub const sessionsPath = ProviderExports.sessionsPath;

fn parseSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const provider.ParseContext,
    runtime: *const provider.ParseRuntime,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*provider.MessageDeduper,
    timezone_offset_minutes: i32,
    sink: provider.EventSink,
) !void {
    if (std.mem.endsWith(u8, file_path, "logs.json")) return;

    _ = deduper;

    const Tokens = struct {
        input: ?u64 = null,
        cached: ?u64 = null,
        output: ?u64 = null,
        tool: ?u64 = null,
        thoughts: ?u64 = null,
        total: ?u64 = null,
    };

    const Message = struct {
        id: ?[]const u8 = null,
        timestamp: ?[]const u8 = null,
        model: ?[]const u8 = null,
        tokens: ?Tokens = null,
    };

    const SessionDoc = struct {
        sessionId: ?[]const u8 = null,
        messages: []Message = &.{},
    };

    var session_label = session_id;
    var session_label_overridden = false;
    var previous_totals: ?RawUsage = null;
    var model_state = ModelState{};

    const Handler = struct {
        allocator: std.mem.Allocator,
        ctx: *const provider.ParseContext,
        session_label: *[]const u8,
        session_label_overridden: *bool,
        timezone_offset_minutes: i32,
        sink: provider.EventSink,
        previous_totals: *?RawUsage,
        model_state: *ModelState,

        fn run(self: *@This(), scratch: std.mem.Allocator, reader: *std.json.Reader) !void {
            var parsed = try std.json.parseFromTokenSource(SessionDoc, scratch, reader, .{
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            if (parsed.value.sessionId) |sid| {
                provider.overrideSessionLabelFromSlice(
                    self.allocator,
                    self.session_label,
                    self.session_label_overridden,
                    sid,
                );
            }

            for (parsed.value.messages) |msg| {
                const ts = msg.timestamp orelse continue;
                const raw_model = msg.model orelse continue;
                const tok = msg.tokens orelse continue;

                var usage_raw = RawUsage{
                    .input_tokens = tok.input orelse 0,
                    .cache_creation_input_tokens = 0,
                    .cached_input_tokens = tok.cached orelse 0,
                    .output_tokens = (tok.output orelse 0) + (tok.tool orelse 0),
                    .reasoning_output_tokens = tok.thoughts orelse 0,
                    .total_tokens = tok.total orelse 0,
                };
                if (usage_raw.total_tokens == 0) {
                    usage_raw.total_tokens =
                        usage_raw.input_tokens +|
                        usage_raw.cache_creation_input_tokens +|
                        usage_raw.cached_input_tokens +|
                        usage_raw.output_tokens +|
                        usage_raw.reasoning_output_tokens;
                }

                const timestamp_info = (try provider.timestampFromSlice(
                    self.allocator,
                    ts,
                    self.timezone_offset_minutes,
                )) orelse continue;

                if (self.previous_totals.*) |prev| {
                    if (std.meta.eql(prev, usage_raw)) {
                        self.allocator.free(timestamp_info.text);
                        continue;
                    }
                }

                var delta = model.TokenUsage.deltaFrom(usage_raw, self.previous_totals.*);
                self.ctx.normalizeUsageDelta(&delta);
                self.previous_totals.* = usage_raw;

                var slot: ?provider.TimestampInfo = timestamp_info;
                defer if (slot) |info| self.allocator.free(info.text);
                try provider.emitUsageEventWithTimestamp(
                    self.ctx,
                    self.allocator,
                    self.model_state,
                    self.sink,
                    self.session_label.*,
                    &slot,
                    delta,
                    raw_model,
                );
            }
        }
    };

    var handler = Handler{
        .allocator = allocator,
        .ctx = ctx,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .sink = sink,
        .previous_totals = &previous_totals,
        .model_state = &model_state,
    };

    try provider.withJsonDocumentReader(
        allocator,
        ctx,
        runtime,
        file_path,
        .{
            .max_bytes = 1024 * 1024 * 1024,
            .open_error_message = "failed to open gemini session file",
            .stat_error_message = "error while statting gemini session file",
        },
        &handler,
        Handler.run,
    );
}

test "gemini parser converts message totals into usage deltas" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);
    var sink_adapter = provider.EventListCollector.init(&events, worker_allocator);
    const sink = sink_adapter.asSink();

    const ctx = provider.ParseContext{
        .provider_name = "gemini-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const runtime = provider.ParseRuntime{ .io = io_single.io() };

    try parseSessionFile(
        worker_allocator,
        &ctx,
        &runtime,
        "gemini-fixture",
        "fixtures/gemini/basic.json",
        null,
        0,
        sink,
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
    try std.testing.expectEqual(@as(u64, 4645), event.usage.total_tokens);
    try std.testing.expectEqual(@as(u64, 4500), event.display_input_tokens);
}
