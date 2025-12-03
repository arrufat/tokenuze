const std = @import("std");
const testing = std.testing;

const model = @import("../model.zig");
const provider = @import("provider.zig");

pub fn makeCapturingConsumer(events: *std.ArrayList(model.TokenUsageEvent)) provider.EventConsumer {
    return .{
        .context = events,
        .ingest = struct {
            fn ingest(
                ctx_ptr: *anyopaque,
                alloc: std.mem.Allocator,
                event: *const model.TokenUsageEvent,
                filters: model.DateFilters,
            ) model.IngestError!void {
                _ = filters;
                const list: *std.ArrayList(model.TokenUsageEvent) = @ptrCast(@alignCast(ctx_ptr));
                var copy = event.*;
                copy.session_id = try alloc.dupe(u8, event.session_id);
                copy.timestamp = try alloc.dupe(u8, event.timestamp);
                copy.model = try alloc.dupe(u8, event.model);
                try list.append(alloc, copy);
            }
        }.ingest,
    };
}

pub fn freeCapturedEvents(allocator: std.mem.Allocator, events: *std.ArrayList(model.TokenUsageEvent)) void {
    for (events.items) |ev| {
        allocator.free(ev.session_id);
        allocator.free(ev.timestamp);
        allocator.free(ev.model);
    }
    events.deinit(allocator);
}

pub fn runFixtureParse(
    allocator: std.mem.Allocator,
    fixture_path: []const u8,
    parse_fn: fn (
        std.mem.Allocator,
        std.mem.Allocator,
        model.DateFilters,
        provider.EventConsumer,
        []const u8,
    ) anyerror!void,
) !usize {
    const json_payload = try std.fs.cwd().readFileAlloc(
        fixture_path,
        allocator,
        .limited(1 << 20),
    );
    defer allocator.free(json_payload);

    var events = std.ArrayList(model.TokenUsageEvent).empty;
    defer freeCapturedEvents(allocator, &events);

    const consumer = makeCapturingConsumer(&events);

    try parse_fn(allocator, allocator, .{}, consumer, json_payload);
    return events.items.len;
}
