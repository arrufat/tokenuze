const std = @import("std");
const Model = @import("model.zig");
const codex = @import("providers/codex.zig");
const gemini = @import("providers/gemini.zig");
const render = @import("render.zig");
pub const machine_id = @import("machine_id.zig");

pub const std_options = .{
    .log_level = .info,
};

pub const DateFilters = Model.DateFilters;
pub const ParseDateError = Model.ParseDateError;
pub const ModelSummary = Model.ModelSummary;
pub const DailySummary = Model.DailySummary;
pub const SummaryTotals = Model.SummaryTotals;
pub const parseFilterDate = Model.parseFilterDate;

pub const ProviderId = enum {
    codex,
    gemini,
};

pub const ProviderSelection = struct {
    include_codex: bool = true,
    include_gemini: bool = true,

    pub fn includes(self: ProviderSelection, id: ProviderId) bool {
        return switch (id) {
            .codex => self.include_codex,
            .gemini => self.include_gemini,
        };
    }
};

const CollectFn = *const fn (
    std.mem.Allocator,
    std.mem.Allocator,
    *Model.SummaryBuilder,
    Model.DateFilters,
    *Model.PricingMap,
    ?std.Progress.Node,
) anyerror!void;

const ProviderSpec = struct {
    id: ProviderId,
    phase_label: []const u8,
    collect: CollectFn,
};

pub fn run(allocator: std.mem.Allocator, filters: DateFilters, selection: ProviderSelection) !void {
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

    var pricing_map = Model.PricingMap.init(allocator);
    defer pricing_map.deinit();

    var summary_builder = Model.SummaryBuilder.init(allocator);
    defer summary_builder.deinit(allocator);

    const providers = [_]ProviderSpec{
        .{ .id = .codex, .phase_label = "collect codex", .collect = codex.collect },
        .{ .id = .gemini, .phase_label = "collect gemini", .collect = gemini.collect },
    };

    for (providers) |provider| {
        if (!selection.includes(provider.id)) continue;
        const before_events = summary_builder.eventCount();
        const before_pricing = pricing_map.count();
        var collect_phase = try PhaseTracker.start(progress_parent, provider.phase_label, 0);
        try provider.collect(allocator, arena, &summary_builder, filters, &pricing_map, collect_phase.progress());
        const elapsed = collect_phase.elapsedMs();
        collect_phase.finish();
        std.log.info(
            "phase.{s} completed in {d:.2}ms (events += {d}, total_events={d}, pricing_models={d}, pricing_added={d})",
            .{
                provider.phase_label,
                elapsed,
                summary_builder.eventCount() - before_events,
                summary_builder.eventCount(),
                pricing_map.count(),
                pricing_map.count() - before_pricing,
            },
        );
    }

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out_writer = &stdout_writer.interface;

    var summaries = summary_builder.items();
    if (summaries.len == 0) {
        std.log.info("no events to process; total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
        var totals = SummaryTotals.init();
        defer totals.deinit(allocator);
        try render.Renderer.writeSummary(out_writer, summaries, &totals, filters.pretty_output);
        try flushOutput(out_writer);
        if (!disable_progress) std.Progress.setStatus(.success);
        return;
    }

    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();

    var pricing_phase = try PhaseTracker.start(progress_parent, "apply pricing", summaries.len);
    defer pricing_phase.finish();
    for (summaries) |*summary| {
        Model.applyPricing(allocator, summary, &pricing_map, &missing_set);
        std.sort.pdq(ModelSummary, summary.models.items, {}, modelLessThan);
        if (pricing_phase.progress()) |node| std.Progress.Node.completeOne(node);
    }
    std.log.info(
        "phase.apply_pricing completed in {d:.2}ms (days={d})",
        .{ pricing_phase.elapsedMs(), summaries.len },
    );

    var sort_days_phase = try PhaseTracker.start(progress_parent, "sort days", 0);
    defer sort_days_phase.finish();
    std.sort.pdq(DailySummary, summaries, {}, summaryLessThan);
    std.log.info(
        "phase.sort_days completed in {d:.2}ms (days={d})",
        .{ sort_days_phase.elapsedMs(), summaries.len },
    );

    var totals = SummaryTotals.init();
    defer totals.deinit(allocator);
    var totals_phase = try PhaseTracker.start(progress_parent, "totals", 0);
    defer totals_phase.finish();
    Model.accumulateTotals(allocator, summaries, &totals);
    try Model.collectMissingModels(allocator, &missing_set, &totals.missing_pricing);
    std.log.info(
        "phase.totals completed in {d:.2}ms (missing_pricing={d})",
        .{ totals_phase.elapsedMs(), totals.missing_pricing.items.len },
    );

    var output_phase = try PhaseTracker.start(progress_parent, "write output", 0);
    defer output_phase.finish();
    try render.Renderer.writeSummary(out_writer, summaries, &totals, filters.pretty_output);
    std.log.info(
        "phase.write_json completed in {d:.2}ms (days={d})",
        .{ output_phase.elapsedMs(), summaries.len },
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
