const std = @import("std");
const Model = @import("model.zig");
const claude = @import("providers/claude.zig");
const codex = @import("providers/codex.zig");
const gemini = @import("providers/gemini.zig");
const render = @import("render.zig");
pub const machine_id = @import("machine_id.zig");
pub const uploader = @import("upload.zig");

pub const std_options = .{
    .log_level = .info,
};

pub const DateFilters = Model.DateFilters;
pub const ParseDateError = Model.ParseDateError;
pub const ModelSummary = Model.ModelSummary;
pub const DailySummary = Model.DailySummary;
pub const SummaryTotals = Model.SummaryTotals;
pub const parseFilterDate = Model.parseFilterDate;

const CollectFn = *const fn (
    std.mem.Allocator,
    std.mem.Allocator,
    *Model.SummaryBuilder,
    Model.DateFilters,
    *Model.PricingMap,
    ?std.Progress.Node,
) anyerror!void;

pub const ProviderSpec = struct {
    name: []const u8,
    phase_label: []const u8,
    collect: CollectFn,
};

pub const providers = [_]ProviderSpec{
    .{ .name = "claude", .phase_label = "collect_claude", .collect = claude.collect },
    .{ .name = "codex", .phase_label = "collect_codex", .collect = codex.collect },
    .{ .name = "gemini", .phase_label = "collect_gemini", .collect = gemini.collect },
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

const ArrayWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    fn init(list: *std.ArrayList(u8), allocator: std.mem.Allocator) ArrayWriter {
        return .{
            .base = .{
                .vtable = &.{
                    .drain = ArrayWriter.drain,
                    .sendFile = std.Io.Writer.unimplementedSendFile,
                    .flush = ArrayWriter.flush,
                    .rebase = std.Io.Writer.defaultRebase,
                },
                .buffer = &.{},
            },
            .list = list,
            .allocator = allocator,
        };
    }

    fn writer(self: *ArrayWriter) *std.Io.Writer {
        return &self.base;
    }

    fn drain(
        writer_ptr: *std.Io.Writer,
        data: []const []const u8,
        splat: usize,
    ) std.Io.Writer.Error!usize {
        const self: *ArrayWriter = @fieldParentPtr("base", writer_ptr);
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
    var summary = try collectSummary(allocator, filters, selection, false);
    defer summary.deinit(allocator);

    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);
    var writer_state = ArrayWriter.init(&buffer, allocator);
    try render.Renderer.writeSummary(writer_state.writer(), summary.builder.items(), &summary.totals, filters.pretty_output);
    return buffer.toOwnedSlice(allocator);
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
    var summary_builder = Model.SummaryBuilder.init(allocator);
    errdefer summary_builder.deinit(allocator);

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

    for (providers, 0..) |provider, idx| {
        if (!selection.includesIndex(idx)) continue;
        const before_events = summary_builder.eventCount();
        const before_pricing = pricing_map.count();
        const stats = blk: {
            var collect_phase = try PhaseTracker.start(progress_parent, provider.phase_label, 0);
            defer collect_phase.finish();
            try provider.collect(allocator, temp_allocator, &summary_builder, filters, &pricing_map, collect_phase.progress());
            break :blk .{
                .elapsed = collect_phase.elapsedMs(),
                .events_added = summary_builder.eventCount() - before_events,
                .total_events = summary_builder.eventCount(),
                .pricing_total = pricing_map.count(),
                .pricing_added = pricing_map.count() - before_pricing,
            };
        };
        std.log.info(
            "phase.{s} completed in {d:.2}ms (events += {d}, total_events={d}, pricing_models={d}, pricing_added={d})",
            .{
                provider.phase_label,
                stats.elapsed,
                stats.events_added,
                stats.total_events,
                stats.pricing_total,
                stats.pricing_added,
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
        Model.accumulateTotals(allocator, summaries, &totals);
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

fn summaryLessThan(_: void, lhs: DailySummary, rhs: DailySummary) bool {
    return std.mem.lessThan(u8, lhs.iso_date, rhs.iso_date);
}

fn modelLessThan(_: void, lhs: ModelSummary, rhs: ModelSummary) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}
