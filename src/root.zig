const std = @import("std");
const Model = @import("model.zig");
const claude = @import("providers/claude.zig");
const codex = @import("providers/codex.zig");
const gemini = @import("providers/gemini.zig");
const session_provider = @import("providers/session_provider.zig");
const render = @import("render.zig");
const io_util = @import("io_util.zig");
const timeutil = @import("time.zig");
pub const machine_id = @import("machine_id.zig");
pub const uploader = @import("upload.zig");
const empty_weekly_json = "{\"weekly\":[]}";

pub const std_options = .{
    .log_level = .info,
};

pub const DateFilters = Model.DateFilters;
pub const ParseDateError = Model.ParseDateError;
pub const ModelSummary = Model.ModelSummary;
pub const DailySummary = Model.DailySummary;
pub const SummaryTotals = Model.SummaryTotals;
pub const parseFilterDate = Model.parseFilterDate;
pub const parseTimezoneOffsetMinutes = timeutil.parseTimezoneOffsetMinutes;
pub const detectLocalTimezoneOffsetMinutes = timeutil.detectLocalTimezoneOffsetMinutes;
pub const DEFAULT_TIMEZONE_OFFSET_MINUTES = timeutil.DEFAULT_TIMEZONE_OFFSET_MINUTES;
pub const formatTimezoneLabel = timeutil.formatTimezoneLabel;

const CollectFn = *const fn (
    std.mem.Allocator,
    std.mem.Allocator,
    *Model.SummaryBuilder,
    Model.DateFilters,
    *Model.PricingMap,
    ?std.Progress.Node,
) anyerror!void;

const LoadPricingFn = *const fn (
    std.mem.Allocator,
    std.mem.Allocator,
    *Model.PricingMap,
) anyerror!void;

pub const ProviderSpec = struct {
    name: []const u8,
    phase_label: []const u8,
    collect: CollectFn,
    load_pricing: LoadPricingFn,
};

pub const providers = [_]ProviderSpec{
    .{
        .name = "claude",
        .phase_label = "collect_claude",
        .collect = claude.collect,
        .load_pricing = claude.loadPricingData,
    },
    .{
        .name = "codex",
        .phase_label = "collect_codex",
        .collect = codex.collect,
        .load_pricing = codex.loadPricingData,
    },
    .{
        .name = "gemini",
        .phase_label = "collect_gemini",
        .collect = gemini.collect,
        .load_pricing = gemini.loadPricingData,
    },
};

const provider_name_list = initProviderNames();
pub const provider_list_description = initProviderListDescription();

const SummaryResult = struct {
    builder: Model.SummaryBuilder,
    totals: Model.SummaryTotals,

    fn deinit(self: *SummaryResult, allocator: std.mem.Allocator) void {
        self.builder.deinit(allocator);
        self.totals.deinit(allocator);
    }
};

pub const UploadReport = struct {
    daily_json: []u8,
    sessions_json: []u8,
    weekly_json: []u8,

    pub fn deinit(self: *UploadReport, allocator: std.mem.Allocator) void {
        allocator.free(self.daily_json);
        allocator.free(self.sessions_json);
        allocator.free(self.weekly_json);
        self.* = undefined;
    }
};

fn initProviderNames() [providers.len][]const u8 {
    var names: [providers.len][]const u8 = undefined;
    inline for (providers, 0..) |provider, idx| {
        names[idx] = provider.name;
    }
    return names;
}

pub fn providerNames() []const []const u8 {
    return provider_name_list[0..];
}

fn initProviderListDescription() []const u8 {
    comptime var combined: []const u8 = "";
    inline for (providers, 0..) |provider, idx| {
        if (idx == 0) {
            combined = provider.name;
        } else {
            combined = std.fmt.comptimePrint("{s}, {s}", .{ combined, provider.name });
        }
    }
    return combined;
}

const provider_count = providers.len;
comptime {
    if (provider_count == 0) @compileError("tokenuze requires at least one provider");
    if (provider_count > 64) @compileError("ProviderSelection mask currently supports up to 64 providers");
}

const provider_full_mask: u64 = if (provider_count == 64)
    std.math.maxInt(u64)
else if (provider_count == 0)
    0
else
    (@as(u64, 1) << @intCast(provider_count)) - 1;

pub const ProviderSelection = struct {
    mask: u64 = provider_full_mask,

    pub fn initAll() ProviderSelection {
        return .{ .mask = provider_full_mask };
    }

    pub fn initEmpty() ProviderSelection {
        return .{ .mask = 0 };
    }

    pub fn includeIndex(self: *ProviderSelection, index: usize) void {
        self.mask |= @as(u64, 1) << @intCast(index);
    }

    pub fn includesIndex(self: ProviderSelection, index: usize) bool {
        return (self.mask & (@as(u64, 1) << @intCast(index))) != 0;
    }

    pub fn isEmpty(self: ProviderSelection) bool {
        return self.mask == 0;
    }
};

pub fn findProviderIndex(name: []const u8) ?usize {
    for (providers, 0..) |provider, idx| {
        if (std.mem.eql(u8, provider.name, name)) return idx;
    }
    return null;
}

pub fn run(allocator: std.mem.Allocator, filters: DateFilters, selection: ProviderSelection) !void {
    const enable_progress = std.fs.File.stdout().isTty();
    var summary = try collectSummary(allocator, filters, selection, enable_progress);
    defer summary.deinit(allocator);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out_writer = &stdout_writer.interface;
    try render.Renderer.writeSummary(out_writer, summary.builder.items(), &summary.totals, filters.pretty_output);
    try flushOutput(out_writer);
}

pub fn renderSummaryAlloc(allocator: std.mem.Allocator, filters: DateFilters, selection: ProviderSelection) ![]u8 {
    var summary = try collectSummaryInternal(allocator, filters, selection, false, null);
    defer summary.deinit(allocator);
    return try renderSummaryBuffer(allocator, summary.builder.items(), &summary.totals, filters.pretty_output);
}

pub fn collectUploadReport(
    allocator: std.mem.Allocator,
    filters: DateFilters,
    selection: ProviderSelection,
) !UploadReport {
    var recorder = Model.SessionRecorder.init(allocator);
    defer recorder.deinit(allocator);

    var summary = try collectSummaryInternal(allocator, filters, selection, false, &recorder);
    defer summary.deinit(allocator);

    const daily_json = try renderSummaryBuffer(allocator, summary.builder.items(), &summary.totals, filters.pretty_output);
    errdefer allocator.free(daily_json);

    const sessions_json = try recorder.renderJson(allocator);
    errdefer allocator.free(sessions_json);

    const weekly_json = try allocator.dupe(u8, empty_weekly_json);
    errdefer allocator.free(weekly_json);

    return UploadReport{
        .daily_json = daily_json,
        .sessions_json = sessions_json,
        .weekly_json = weekly_json,
    };
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

fn collectSummary(
    allocator: std.mem.Allocator,
    filters: DateFilters,
    selection: ProviderSelection,
    enable_progress: bool,
) !SummaryResult {
    return collectSummaryInternal(allocator, filters, selection, enable_progress, null);
}

fn collectSummaryInternal(
    allocator: std.mem.Allocator,
    filters: DateFilters,
    selection: ProviderSelection,
    enable_progress: bool,
    session_recorder: ?*Model.SessionRecorder,
) !SummaryResult {
    var summary_builder = Model.SummaryBuilder.init(allocator);
    errdefer summary_builder.deinit(allocator);
    if (session_recorder) |recorder| summary_builder.attachSessionRecorder(recorder);

    var totals = SummaryTotals.init();
    errdefer totals.deinit(allocator);

    var pricing_map = Model.PricingMap.init(allocator);
    defer Model.deinitPricingMap(&pricing_map, allocator);

    const temp_allocator = std.heap.page_allocator;

    var progress_root: std.Progress.Node = undefined;
    const progress_parent = if (enable_progress) blk: {
        progress_root = std.Progress.start(.{ .root_name = "Tokenuze" });
        break :blk &progress_root;
    } else null;
    defer if (enable_progress) std.Progress.Node.end(progress_root);
    if (enable_progress) {
        errdefer std.Progress.setStatus(.failure);
    }

    var total_timer = try std.time.Timer.start();

    if (!selection.isEmpty()) {
        try loadPricingOnce(allocator, temp_allocator, selection, &pricing_map, progress_parent);
    }

    for (providers, 0..) |provider, idx| {
        if (!selection.includesIndex(idx)) continue;
        const before_events = summary_builder.eventCount();
        const stats = blk: {
            var collect_phase = try PhaseTracker.start(progress_parent, provider.phase_label, 0);
            defer collect_phase.finish();
            try provider.collect(allocator, temp_allocator, &summary_builder, filters, &pricing_map, collect_phase.progress());
            break :blk .{
                .elapsed = collect_phase.elapsedMs(),
                .events_added = summary_builder.eventCount() - before_events,
                .total_events = summary_builder.eventCount(),
            };
        };
        std.log.info(
            "phase.{s} completed in {d:.2}ms (events += {d}, total_events={d})",
            .{
                provider.phase_label,
                stats.elapsed,
                stats.events_added,
                stats.total_events,
            },
        );
    }

    var summaries = summary_builder.items();
    if (summaries.len == 0) {
        std.log.info("no events to process; total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
        if (enable_progress) std.Progress.setStatus(.success);
        return SummaryResult{ .builder = summary_builder, .totals = totals };
    }

    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();

    const pricing_elapsed = blk: {
        var pricing_phase = try PhaseTracker.start(progress_parent, "apply pricing", summaries.len);
        defer pricing_phase.finish();
        for (summaries) |*summary| {
            Model.applyPricing(allocator, summary, &pricing_map, &missing_set);
            std.sort.pdq(ModelSummary, summary.models.items, {}, modelLessThan);
            if (pricing_phase.progress()) |node| std.Progress.Node.completeOne(node);
        }
        break :blk pricing_phase.elapsedMs();
    };
    std.log.info(
        "phase.apply_pricing completed in {d:.2}ms (days={d})",
        .{ pricing_elapsed, summaries.len },
    );
    if (session_recorder) |recorder| {
        recorder.applyPricing(&pricing_map);
    }

    const sort_elapsed = blk: {
        var sort_days_phase = try PhaseTracker.start(progress_parent, "sort days", 0);
        defer sort_days_phase.finish();
        std.sort.pdq(DailySummary, summaries, {}, summaryLessThan);
        break :blk sort_days_phase.elapsedMs();
    };
    std.log.info(
        "phase.sort_days completed in {d:.2}ms (days={d})",
        .{ sort_elapsed, summaries.len },
    );

    const totals_elapsed = blk: {
        var totals_phase = try PhaseTracker.start(progress_parent, "totals", 0);
        defer totals_phase.finish();
        Model.accumulateTotals(summaries, &totals);
        try Model.collectMissingModels(allocator, &missing_set, &totals.missing_pricing);
        break :blk totals_phase.elapsedMs();
    };
    std.log.info(
        "phase.totals completed in {d:.2}ms (missing_pricing={d})",
        .{ totals_elapsed, totals.missing_pricing.items.len },
    );

    std.log.info("phase.total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
    if (enable_progress) std.Progress.setStatus(.success);

    return SummaryResult{ .builder = summary_builder, .totals = totals };
}

fn loadPricingOnce(
    allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    selection: ProviderSelection,
    pricing: *Model.PricingMap,
    progress_parent: ?*std.Progress.Node,
) !void {
    var pricing_phase = try PhaseTracker.start(progress_parent, "load pricing", 0);
    defer pricing_phase.finish();

    const before_models = pricing.count();
    const remote_stats = try session_provider.loadRemotePricingOnce(allocator, temp_allocator, pricing);
    if (remote_stats.attempted) {
        if (remote_stats.failure) |err| {
            std.log.warn(
                "pricing.remote_fetch failed after {d:.2}ms ({s})",
                .{ remote_stats.elapsed_ms, @errorName(err) },
            );
        } else {
            std.log.info(
                "pricing.remote_fetch completed in {d:.2}ms (models += {d})",
                .{ remote_stats.elapsed_ms, remote_stats.models_added },
            );
        }
    } else {
        std.log.info("pricing.remote_fetch skipped (already loaded)", .{});
    }

    var fallback_timer = try std.time.Timer.start();
    for (providers, 0..) |provider, idx| {
        if (!selection.includesIndex(idx)) continue;
        try provider.load_pricing(allocator, temp_allocator, pricing);
    }
    const fallback_elapsed = nsToMs(fallback_timer.read());
    const fallback_added = pricing.count() - (before_models + remote_stats.models_added);
    std.log.info(
        "pricing.fallback ensured in {d:.2}ms (models += {d})",
        .{ fallback_elapsed, fallback_added },
    );

    std.log.info(
        "phase.load_pricing completed in {d:.2}ms (models={d}, models_added={d})",
        .{ pricing_phase.elapsedMs(), pricing.count(), pricing.count() - before_models },
    );
}

fn summaryLessThan(_: void, lhs: DailySummary, rhs: DailySummary) bool {
    return std.mem.lessThan(u8, lhs.iso_date, rhs.iso_date);
}

fn modelLessThan(_: void, lhs: ModelSummary, rhs: ModelSummary) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn renderSummaryBuffer(
    allocator: std.mem.Allocator,
    summaries: []const Model.DailySummary,
    totals: *const Model.SummaryTotals,
    pretty: bool,
) ![]u8 {
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);
    var writer_state = io_util.ArrayWriter.init(&buffer, allocator);
    try render.Renderer.writeSummary(writer_state.writer(), summaries, totals, pretty);
    return buffer.toOwnedSlice(allocator);
}
