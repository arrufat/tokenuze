const std = @import("std");
const Model = @import("model.zig");
const codex = @import("providers/codex.zig");
const render = @import("render.zig");

pub const std_options = .{
    .log_level = .info,
};

pub const DateFilters = Model.DateFilters;
pub const ParseDateError = Model.ParseDateError;
pub const ModelSummary = Model.ModelSummary;
pub const DailySummary = Model.DailySummary;
pub const SummaryTotals = Model.SummaryTotals;
pub const parseFilterDate = Model.parseFilterDate;

pub fn run(allocator: std.mem.Allocator, filters: DateFilters) !void {
    const disable_progress = !std.fs.File.stdout().isTty();
    var progress_root: std.Progress.Node = undefined;
    if (!disable_progress) {
        progress_root = std.Progress.start(.{ .root_name = "Tokenuze" });
    }
    defer if (!disable_progress) std.Progress.Node.end(progress_root);
    errdefer if (!disable_progress) std.Progress.setStatus(.failure);
    const progress_parent = if (!disable_progress) &progress_root else null;

    var total_timer = try std.time.Timer.start();

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var events = std.ArrayListUnmanaged(Model.TokenUsageEvent){};
    defer events.deinit(allocator);

    var pricing_map = Model.PricingMap.init(allocator);
    defer pricing_map.deinit();

    var collect_phase = try PhaseTracker.start(progress_parent, "collect codex", 0);
    defer collect_phase.finish();
    try codex.collect(allocator, arena, &events, &pricing_map, collect_phase.progress());
    std.log.info(
        "phase.collect completed in {d:.2}ms (events={d}, pricing_models={d})",
        .{ collect_phase.elapsedMs(), events.items.len, pricing_map.count() },
    );

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out_writer = &stdout_writer.interface;

    if (events.items.len == 0) {
        std.log.info("no events to process; total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
        var totals = SummaryTotals.init();
        defer totals.deinit(allocator);
        const empty_days = [_]DailySummary{};
        try render.Renderer.writeSummary(out_writer, empty_days[0..], &totals, filters.pretty_output);
        try flushOutput(out_writer);
        if (!disable_progress) std.Progress.setStatus(.success);
        return;
    }

    // ... rest of file ...

    var summaries = std.ArrayListUnmanaged(DailySummary){};
    defer {
        for (summaries.items) |*summary| {
            summary.deinit(allocator);
        }
        summaries.deinit(allocator);
    }

    var date_index = std.StringHashMap(usize).init(allocator);
    defer date_index.deinit();

    var build_phase = try PhaseTracker.start(progress_parent, "build summaries", 0);
    defer build_phase.finish();
    try Model.buildDailySummaries(allocator, arena, events.items, &summaries, &date_index, filters);
    std.log.info(
        "phase.build_summaries completed in {d:.2}ms (days={d})",
        .{ build_phase.elapsedMs(), summaries.items.len },
    );

    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();

    var pricing_phase = try PhaseTracker.start(progress_parent, "apply pricing", summaries.items.len);
    defer pricing_phase.finish();
    for (summaries.items) |*summary| {
        Model.applyPricing(allocator, summary, &pricing_map, &missing_set);
        std.sort.pdq(ModelSummary, summary.models.items, {}, modelLessThan);
        if (pricing_phase.progress()) |node| std.Progress.Node.completeOne(node);
    }
    std.log.info(
        "phase.apply_pricing completed in {d:.2}ms (days={d})",
        .{ pricing_phase.elapsedMs(), summaries.items.len },
    );

    var sort_days_phase = try PhaseTracker.start(progress_parent, "sort days", 0);
    defer sort_days_phase.finish();
    std.sort.pdq(DailySummary, summaries.items, {}, summaryLessThan);
    std.log.info(
        "phase.sort_days completed in {d:.2}ms (days={d})",
        .{ sort_days_phase.elapsedMs(), summaries.items.len },
    );

    var totals = SummaryTotals.init();
    defer totals.deinit(allocator);
    var totals_phase = try PhaseTracker.start(progress_parent, "totals", 0);
    defer totals_phase.finish();
    Model.accumulateTotals(allocator, &summaries, &totals);
    try Model.collectMissingModels(allocator, &missing_set, &totals.missing_pricing);
    std.log.info(
        "phase.totals completed in {d:.2}ms (missing_pricing={d})",
        .{ totals_phase.elapsedMs(), totals.missing_pricing.items.len },
    );

    var output_phase = try PhaseTracker.start(progress_parent, "write output", 0);
    defer output_phase.finish();
    try render.Renderer.writeSummary(out_writer, summaries.items, &totals, filters.pretty_output);
    std.log.info(
        "phase.write_json completed in {d:.2}ms (days={d})",
        .{ output_phase.elapsedMs(), summaries.items.len },
    );

    try flushOutput(out_writer);
    std.log.info("phase.total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
    if (!disable_progress) std.Progress.setStatus(.success);
}

const PhaseTracker = struct {
    timer: std.time.Timer,
    node: ?std.Progress.Node,

    pub fn start(parent: ?*std.Progress.Node, label: []const u8, total_items: usize) !PhaseTracker {
        return .{
            .timer = try std.time.Timer.start(),
            .node = if (parent) |root| std.Progress.Node.start(root.*, label, total_items) else null,
        };
    }

    pub fn progress(self: *PhaseTracker) ?std.Progress.Node {
        return self.node;
    }

    pub fn elapsedMs(self: *PhaseTracker) f64 {
        return nsToMs(self.timer.read());
    }

    pub fn finish(self: *PhaseTracker) void {
        if (self.node) |node| std.Progress.Node.end(node);
    }
};

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn flushOutput(writer: anytype) !void {
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}

fn summaryLessThan(_: void, lhs: DailySummary, rhs: DailySummary) bool {
    return std.mem.lessThan(u8, lhs.iso_date, rhs.iso_date);
}

fn modelLessThan(_: void, lhs: ModelSummary, rhs: ModelSummary) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}
