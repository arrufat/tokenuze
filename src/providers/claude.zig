const std = @import("std");
const model = @import("../model.zig");
const timeutil = @import("../time.zig");
const SessionProvider = @import("session_provider.zig");

const RawUsage = model.RawTokenUsage;
const MessageDeduper = SessionProvider.MessageDeduper;
const ModelState = SessionProvider.ModelState;

const ProviderExports = SessionProvider.makeProvider(.{
    .name = "claude",
    .sessions_dir_suffix = "/.claude/projects",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".jsonl",
    .cached_counts_overlap_input = false,
    .parse_session_fn = parseSessionFile,
    .requires_deduper = true,
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
    try parseClaudeSessionFile(allocator, ctx, session_id, file_path, deduper, timezone_offset_minutes, events);
}

fn parseClaudeSessionFile(
    allocator: std.mem.Allocator,
    ctx: *const SessionProvider.ParseContext,
    session_id: []const u8,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
) !void {
    var session_label = session_id;
    var session_label_overridden = false;
    var model_state = ModelState{};

    var handler = ClaudeLineHandler{
        .ctx = ctx,
        .allocator = allocator,
        .file_path = file_path,
        .deduper = deduper,
        .session_label = &session_label,
        .session_label_overridden = &session_label_overridden,
        .timezone_offset_minutes = timezone_offset_minutes,
        .events = events,
        .model_state = &model_state,
    };

    try SessionProvider.streamJsonLines(
        allocator,
        ctx,
        file_path,
        .{
            .max_bytes = 128 * 1024 * 1024,
            .open_error_message = "unable to open claude session file",
            .read_error_message = "error while reading claude session stream",
            .advance_error_message = "error while advancing claude session stream",
        },
        &handler,
        ClaudeLineHandler.handle,
    );
}

const ClaudeLineHandler = struct {
    ctx: *const SessionProvider.ParseContext,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
    model_state: *ModelState,

    fn handle(self: *ClaudeLineHandler, line: []const u8, line_index: usize) !void {
        try handleClaudeLine(
            self.ctx,
            self.allocator,
            line,
            line_index,
            self.file_path,
            self.deduper,
            self.session_label,
            self.session_label_overridden,
            self.timezone_offset_minutes,
            self.events,
            self.model_state,
        );
    }
};

fn handleClaudeLine(
    ctx: *const SessionProvider.ParseContext,
    allocator: std.mem.Allocator,
    line: []const u8,
    line_index: usize,
    file_path: []const u8,
    deduper: ?*MessageDeduper,
    session_label: *[]const u8,
    session_label_overridden: *bool,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
    model_state: *ModelState,
) !void {
    var parsed_doc = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch |err| {
        std.log.warn(
            "{s}: failed to parse claude session file '{s}' line {d} ({s})",
            .{ ctx.provider_name, file_path, line_index, @errorName(err) },
        );
        return;
    };
    defer parsed_doc.deinit();

    const record = switch (parsed_doc.value) {
        .object => |obj| obj,
        else => return,
    };

    if (!session_label_overridden.*) {
        if (record.get("sessionId")) |sid_value| {
            switch (sid_value) {
                .string => |slice| {
                    const duplicate = SessionProvider.duplicateNonEmpty(allocator, slice) catch null;
                    if (duplicate) |dup| {
                        session_label.* = dup;
                        session_label_overridden.* = true;
                    }
                },
                else => {},
            }
        }
    }

    try emitClaudeEvent(
        ctx,
        allocator,
        record,
        deduper,
        session_label.*,
        timezone_offset_minutes,
        events,
        model_state,
    );
}

fn emitClaudeEvent(
    ctx: *const SessionProvider.ParseContext,
    allocator: std.mem.Allocator,
    record: std.json.ObjectMap,
    deduper: ?*MessageDeduper,
    session_label: []const u8,
    timezone_offset_minutes: i32,
    events: *std.ArrayList(model.TokenUsageEvent),
    model_state: *ModelState,
) !void {
    const type_value = record.get("type") orelse return;
    const type_slice = switch (type_value) {
        .string => |slice| slice,
        else => return,
    };
    if (!std.mem.eql(u8, type_slice, "assistant")) return;

    const message_value = record.get("message") orelse return;
    const message_obj = switch (message_value) {
        .object => |obj| obj,
        else => return,
    };

    if (!try shouldEmitClaudeMessage(deduper, record, message_obj)) {
        return;
    }

    const usage_value = message_obj.get("usage") orelse return;
    const usage_obj = switch (usage_value) {
        .object => |obj| obj,
        else => return,
    };

    const timestamp_value = record.get("timestamp") orelse return;
    const timestamp_slice = switch (timestamp_value) {
        .string => |slice| slice,
        else => return,
    };
    const timestamp_copy = SessionProvider.duplicateNonEmpty(allocator, timestamp_slice) catch return;
    const owned_timestamp = timestamp_copy orelse return;
    const iso_date = timeutil.isoDateForTimezone(owned_timestamp, timezone_offset_minutes) catch {
        return;
    };

    const message_model = message_obj.get("model");
    const resolved_model = (try ctx.requireModel(allocator, model_state, message_model)) orelse return;

    const raw = parseClaudeUsage(usage_obj);
    const usage = model.TokenUsage.fromRaw(raw);
    if (usage.input_tokens == 0 and usage.cached_input_tokens == 0 and usage.output_tokens == 0 and usage.reasoning_output_tokens == 0) {
        return;
    }

    const event = model.TokenUsageEvent{
        .session_id = session_label,
        .timestamp = owned_timestamp,
        .local_iso_date = iso_date,
        .model = resolved_model.name,
        .usage = usage,
        .is_fallback = resolved_model.is_fallback,
        .display_input_tokens = ctx.computeDisplayInput(usage),
    };
    try events.append(allocator, event);
}

fn shouldEmitClaudeMessage(
    deduper: ?*MessageDeduper,
    record: std.json.ObjectMap,
    message_obj: std.json.ObjectMap,
) !bool {
    const dedupe = deduper orelse return true;
    const id_value = message_obj.get("id") orelse return true;
    const id_slice = switch (id_value) {
        .string => |slice| slice,
        else => return true,
    };
    const request_value = record.get("requestId") orelse return true;
    const request_slice = switch (request_value) {
        .string => |slice| slice,
        else => return true,
    };
    var hash = std.hash.Wyhash.hash(0, id_slice);
    hash = std.hash.Wyhash.hash(hash, request_slice);
    return try dedupe.mark(hash);
}

fn parseClaudeUsage(usage_obj: std.json.ObjectMap) RawUsage {
    const direct_input = SessionProvider.jsonValueToU64(usage_obj.get("input_tokens"));
    const cache_creation = SessionProvider.jsonValueToU64(usage_obj.get("cache_creation_input_tokens"));
    const cached_reads = SessionProvider.jsonValueToU64(usage_obj.get("cache_read_input_tokens"));
    const output_tokens = SessionProvider.jsonValueToU64(usage_obj.get("output_tokens"));

    const input_total = std.math.add(u64, direct_input, cache_creation) catch std.math.maxInt(u64);
    const with_cached = std.math.add(u64, input_total, cached_reads) catch std.math.maxInt(u64);
    const total_tokens = std.math.add(u64, with_cached, output_tokens) catch std.math.maxInt(u64);

    return .{
        .input_tokens = direct_input,
        .cache_creation_input_tokens = cache_creation,
        .cached_input_tokens = cached_reads,
        .output_tokens = output_tokens,
        .reasoning_output_tokens = 0,
        .total_tokens = total_tokens,
    };
}

test "claude parser emits assistant usage events and respects overrides" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const worker_allocator = arena_state.allocator();

    var events: std.ArrayList(model.TokenUsageEvent) = .empty;
    defer events.deinit(worker_allocator);

    var deduper = try MessageDeduper.init(worker_allocator);
    defer deduper.deinit();

    const ctx = SessionProvider.ParseContext{
        .provider_name = "claude-test",
        .legacy_fallback_model = null,
        .cached_counts_overlap_input = false,
    };

    try parseClaudeSessionFile(
        worker_allocator,
        &ctx,
        "claude-fixture",
        "fixtures/claude/basic.jsonl",
        &deduper,
        0,
        &events,
    );

    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    const event = events.items[0];
    try std.testing.expectEqualStrings("claude-session", event.session_id);
    try std.testing.expectEqualStrings("claude-3-5-sonnet", event.model);
    try std.testing.expect(!event.is_fallback);
    try std.testing.expectEqual(@as(u64, 1500), event.usage.input_tokens);
    try std.testing.expectEqual(@as(u64, 100), event.usage.cache_creation_input_tokens);
    try std.testing.expectEqual(@as(u64, 250), event.usage.cached_input_tokens);
    try std.testing.expectEqual(@as(u64, 600), event.usage.output_tokens);
    try std.testing.expectEqual(@as(u64, 1500), event.display_input_tokens);
}
