const std = @import("std");
const builtin = @import("builtin");

pub const Context = @import("Context.zig");

const io_util = @import("io_util.zig");
pub const machine_id = @import("machine_id.zig");
const model = @import("model.zig");
pub const DateFilters = model.DateFilters;
pub const ParseDateError = model.ParseDateError;
pub const OutputFormat = model.OutputFormat;
pub const ModelSummary = model.ModelSummary;
pub const DailySummary = model.DailySummary;
pub const SummaryTotals = model.SummaryTotals;
pub const parseFilterDate = model.parseFilterDate;
const amp = @import("providers/amp.zig");
const claude = @import("providers/claude.zig");
const codex = @import("providers/codex.zig");
const crush = @import("providers/crush.zig");
const gemini = @import("providers/gemini.zig");
const opencode = @import("providers/opencode.zig");
const provider = @import("providers/provider.zig");
const zed = @import("providers/zed.zig");
const render = @import("render.zig");
const timeutil = @import("time.zig");
pub const parseTimezoneOffsetMinutes = timeutil.parseTimezoneOffsetMinutes;
pub const detectLocalTimezoneOffsetMinutes = timeutil.detectLocalTimezoneOffsetMinutes;
pub const default_timezone_offset_minutes = timeutil.default_timezone_offset_minutes;
pub const local_timezone_sentinel = timeutil.local_timezone_sentinel;
pub const formatTimezoneLabel = timeutil.formatTimezoneLabel;
const nsToMs = timeutil.nsToMs;
pub const uploader = @import("upload.zig");

pub const std_options: std.Options = .{
    // Compile debug logs so they can be enabled dynamically at runtime.
    .log_level = .debug,
    .logFn = logFn,
};

var runtime_log_level: std.atomic.Value(u8) = .init(@intFromEnum(if (builtin.mode == .Debug) std.log.Level.debug else std.log.Level.err));

pub fn setLogLevel(level: std.log.Level) void {
    runtime_log_level.store(@intFromEnum(level), .release);
}

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const current_level: std.log.Level = @enumFromInt(runtime_log_level.load(.acquire));
    if (@intFromEnum(level) > @intFromEnum(current_level)) return;
    std.log.defaultLog(level, scope, format, args);
}

const CollectFn = *const fn (Context, *model.SummaryBuilder, model.DateFilters, ?std.Progress.Node) anyerror!void;

const LoadPricingFn = *const fn (
    std.mem.Allocator,
    *model.PricingMap,
) anyerror!void;

const PathHintFn = *const fn (Context) anyerror![]u8;

pub const ProviderSpec = struct {
    name: []const u8,
    phase_label: []const u8,
    collect: CollectFn,
    load_pricing: LoadPricingFn,
    path_hint: PathHintFn,
};

pub const providers = [_]ProviderSpec{
    .{
        .name = "claude",
        .phase_label = "collect_claude",
        .collect = claude.collect,
        .load_pricing = claude.loadPricingData,
        .path_hint = claude.sessionsPath,
    },
    .{
        .name = "codex",
        .phase_label = "collect_codex",
        .collect = codex.collect,
        .load_pricing = codex.loadPricingData,
        .path_hint = codex.sessionsPath,
    },
    .{
        .name = "amp",
        .phase_label = "collect_amp",
        .collect = amp.collect,
        .load_pricing = amp.loadPricingData,
        .path_hint = amp.sessionsPath,
    },
    .{
        .name = "gemini",
        .phase_label = "collect_gemini",
        .collect = gemini.collect,
        .load_pricing = gemini.loadPricingData,
        .path_hint = gemini.sessionsPath,
    },
    .{
        .name = "opencode",
        .phase_label = "collect_opencode",
        .collect = opencode.collect,
        .load_pricing = opencode.loadPricingData,
        .path_hint = opencode.sessionsPath,
    },
    .{
        .name = "zed",
        .phase_label = "collect_zed",
        .collect = zed.collect,
        .load_pricing = zed.loadPricingData,
        .path_hint = zed.pathHint,
    },
    .{
        .name = "crush",
        .phase_label = "collect_crush",
        .collect = crush.collect,
        .load_pricing = crush.loadPricingData,
        .path_hint = crush.pathHint,
    },
};

const SummaryResult = struct {
    builder: model.SummaryBuilder,
    totals: model.SummaryTotals,

    fn deinit(self: *SummaryResult, allocator: std.mem.Allocator) void {
        self.builder.deinit(allocator);
        self.totals.deinit(allocator);
    }
};

pub const UploadReport = struct {
    daily_json: []u8,
    sessions_json: []u8,

    pub fn deinit(self: *UploadReport, allocator: std.mem.Allocator) void {
        allocator.free(self.daily_json);
        allocator.free(self.sessions_json);
        self.* = undefined;
    }
};

pub const PricingCache = struct {
    map: model.PricingMap,
    loaded_mask: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) PricingCache {
        return .{ .map = model.PricingMap.init(allocator), .loaded_mask = 0 };
    }

    pub fn deinit(self: *PricingCache, allocator: std.mem.Allocator) void {
        model.deinitPricingMap(&self.map, allocator);
        self.* = undefined;
    }

    pub fn ensureLoaded(
        self: *PricingCache,
        allocator: std.mem.Allocator,
        temp_allocator: std.mem.Allocator,
        selection: ProviderSelection,
        progress_parent: ?*std.Progress.Node,
    ) !void {
        const missing_mask = selection.mask & ~self.loaded_mask;
        if (missing_mask == 0) return;

        const missing_selection = ProviderSelection{ .mask = missing_mask };
        try loadPricing(allocator, temp_allocator, missing_selection, &self.map, progress_parent);
        self.loaded_mask |= missing_mask;
    }
};

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
    for (providers, 0..) |prov, idx| {
        if (std.mem.eql(u8, prov.name, name)) return idx;
    }
    return null;
}

fn defaultContext(io: std.Io, environ_map: *const std.process.Environ.Map, allocator: std.mem.Allocator) Context {
    return .{
        .allocator = allocator,
        .temp_allocator = std.heap.page_allocator,
        .io = io,
        .environ_map = environ_map,
    };
}

pub fn run(
    io: std.Io,
    environ_map: *const std.process.Environ.Map,
    allocator: std.mem.Allocator,
    filters: DateFilters,
    selection: ProviderSelection,
) !void {
    return runWithContext(defaultContext(io, environ_map, allocator), filters, selection);
}

pub fn runWithContext(ctx: Context, filters: DateFilters, selection: ProviderSelection) !void {
    const enable_progress = std.Io.File.stdout().isTty(ctx.io) catch false;
    logRunStart(filters, selection, enable_progress);
    var summary = try collectSummary(ctx, filters, selection, enable_progress);
    defer summary.deinit(ctx.allocator);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(ctx.io, stdout_buffer[0..]);
    const out_writer = &stdout_writer.interface;
    switch (filters.output_format) {
        .table => try render.Renderer.writeTable(out_writer, ctx.allocator, summary.builder.items(), &summary.totals),
        .json => try render.Renderer.writeJson(out_writer, summary.builder.items(), &summary.totals, filters.pretty_output),
    }
    try flushOutput(out_writer);
}

pub fn renderSummaryAlloc(
    allocator: std.mem.Allocator,
    environ_map: *const std.process.Environ.Map,
    filters: DateFilters,
    selection: ProviderSelection,
) ![]u8 {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const ctx = defaultContext(io_single.io(), environ_map, allocator);

    var summary = try collectSummaryInternal(ctx, filters, selection, false, null, null);
    defer summary.deinit(allocator);
    return try renderSummaryBuffer(allocator, summary.builder.items(), &summary.totals, filters.pretty_output);
}

pub fn renderSessionsTable(
    writer: *std.Io.Writer,
    allocator: std.mem.Allocator,
    recorder: *const model.SessionRecorder,
    timezone_offset_minutes: i32,
) !void {
    try render.Renderer.writeSessionsTable(writer, allocator, recorder, timezone_offset_minutes);
}

pub fn renderSessionsAlloc(
    allocator: std.mem.Allocator,
    io: std.Io,
    environ_map: *const std.process.Environ.Map,
    filters: DateFilters,
    selection: ProviderSelection,
    pretty: bool,
) ![]u8 {
    var cache = PricingCache.init(allocator);
    defer cache.deinit(allocator);
    return try renderSessionsWithCache(allocator, io, environ_map, filters, selection, pretty, &cache);
}

pub fn renderSessionsWithCache(
    allocator: std.mem.Allocator,
    io: std.Io,
    environ_map: *const std.process.Environ.Map,
    filters: DateFilters,
    selection: ProviderSelection,
    pretty: bool,
    cache: *PricingCache,
) ![]u8 {
    const ctx = defaultContext(io, environ_map, allocator);
    var recorder = try collectSessionsWithCacheWithContext(ctx, filters, selection, cache);
    defer recorder.deinit(allocator);
    return recorder.renderJson(allocator, pretty);
}

pub fn collectSessionsWithCache(
    io: std.Io,
    environ_map: *const std.process.Environ.Map,
    allocator: std.mem.Allocator,
    filters: DateFilters,
    selection: ProviderSelection,
    cache: *PricingCache,
) !model.SessionRecorder {
    return collectSessionsWithCacheWithContext(defaultContext(io, environ_map, allocator), filters, selection, cache);
}

pub fn collectSessionsWithCacheWithContext(
    ctx: Context,
    filters: DateFilters,
    selection: ProviderSelection,
    cache: *PricingCache,
) !model.SessionRecorder {
    var recorder = model.SessionRecorder.init(ctx.allocator);
    errdefer recorder.deinit(ctx.allocator);

    var summary = try collectSummaryInternal(ctx, filters, selection, false, &recorder, cache);
    // Mirror the aggregated totals from the daily summary to keep --sessions output
    // in lockstep with the default summary, even if per-session pricing or
    // grouping would introduce tiny floating-point differences.
    recorder.totals = summary.totals.usage;
    recorder.display_total_input_tokens = summary.totals.display_input_tokens;
    recorder.total_cost_usd = summary.totals.cost_usd;
    defer summary.deinit(ctx.allocator);

    return recorder;
}

pub fn collectUploadReport(ctx: Context, filters: DateFilters, selection: ProviderSelection) !UploadReport {
    var cache = PricingCache.init(ctx.allocator);
    defer cache.deinit(ctx.allocator);
    return try collectUploadReportWithCache(ctx, filters, selection, &cache);
}

pub fn collectUploadReportWithCache(
    ctx: Context,
    filters: DateFilters,
    selection: ProviderSelection,
    cache: *PricingCache,
) !UploadReport {
    var recorder = model.SessionRecorder.init(ctx.allocator);
    defer recorder.deinit(ctx.allocator);

    var summary = try collectSummaryInternal(ctx, filters, selection, false, &recorder, cache);
    defer summary.deinit(ctx.allocator);

    const daily_json = try renderSummaryBuffer(ctx.allocator, summary.builder.items(), &summary.totals, filters.pretty_output);
    errdefer ctx.allocator.free(daily_json);

    const sessions_json = try recorder.renderJson(ctx.allocator, filters.pretty_output);
    errdefer ctx.allocator.free(sessions_json);

    return UploadReport{
        .daily_json = daily_json,
        .sessions_json = sessions_json,
    };
}

const ProviderSelectionSummary = struct {
    names: []const u8,
    count: usize,
};

fn logRunStart(filters: DateFilters, selection: ProviderSelection, enable_progress: bool) void {
    var tz_buf: [16]u8 = undefined;
    const tz_label = formatTimezoneLabel(&tz_buf, filters.timezone_offset_minutes);
    const since_label = if (filters.since) |since| since[0..] else "any";
    const until_label = if (filters.until) |until| until[0..] else "any";

    var provider_buf: [256]u8 = undefined;
    const provider_summary = describeSelectedProviders(selection, &provider_buf);

    std.log.info(
        "run.start since={s} until={s} tz={s} providers={d} pretty={any} progress={any}",
        .{
            since_label,
            until_label,
            tz_label,
            provider_summary.count,
            filters.pretty_output,
            enable_progress,
        },
    );
    std.log.debug("run.providers {s}", .{provider_summary.names});
}

fn describeSelectedProviders(selection: ProviderSelection, buffer: []u8) ProviderSelectionSummary {
    var selected: [provider_count][]const u8 = undefined;
    var count: usize = 0;
    for (providers, 0..) |prov, idx| {
        if (!selection.includesIndex(idx)) continue;
        selected[count] = prov.name;
        count += 1;
    }

    if (count == 0) {
        return .{ .names = "(none)", .count = 0 };
    }

    var fba = std.heap.FixedBufferAllocator.init(buffer);
    const joined = std.mem.join(fba.allocator(), ", ", selected[0..count]) catch {
        const placeholder = "(truncated)";
        if (buffer.len == 0) return .{ .names = "", .count = count };
        const copy_len = @min(placeholder.len, buffer.len);
        @memcpy(buffer[0..copy_len], placeholder[0..copy_len]);
        return .{ .names = buffer[0..copy_len], .count = count };
    };

    return .{ .names = joined, .count = count };
}

pub fn providerListDescription(buffer: []u8) []const u8 {
    return describeSelectedProviders(ProviderSelection.initAll(), buffer).names;
}

pub const ProviderPathInfo = struct {
    name: []const u8,
    path: []const u8,
};

pub fn providerPathInfos(
    allocator: std.mem.Allocator,
    environ_map: *const std.process.Environ.Map,
) !std.ArrayList(ProviderPathInfo) {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();
    const ctx = defaultContext(io_single.io(), environ_map, allocator);

    var list: std.ArrayList(ProviderPathInfo) = .empty;
    errdefer {
        for (list.items) |info| allocator.free(info.path);
        list.deinit(allocator);
    }

    for (providers) |spec| {
        const resolved = spec.path_hint(ctx) catch |err| blk: {
            const note = try std.fmt.allocPrint(allocator, "unavailable: {s}", .{@errorName(err)});
            break :blk note;
        };
        try list.append(allocator, .{ .name = spec.name, .path = resolved });
    }

    std.mem.sort(ProviderPathInfo, list.items, {}, struct {
        fn lessThan(_: void, a: ProviderPathInfo, b: ProviderPathInfo) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);

    return list;
}

fn startProgressNode(parent: ?*std.Progress.Node, label: []const u8, total_items: usize) std.Progress.Node {
    if (parent) |root| {
        return std.Progress.Node.start(root.*, label, total_items);
    }
    return std.Progress.Node.none;
}

fn finishProgressNode(node: std.Progress.Node) void {
    if (node.index != .none) {
        std.Progress.Node.end(node);
    }
}

fn progressHandle(node: std.Progress.Node) ?std.Progress.Node {
    return if (node.index == .none) null else node;
}

fn flushOutput(writer: anytype) !void {
    writer.flush() catch |err| switch (err) {
        error.WriteFailed => {},
        else => return err,
    };
}

fn collectSummary(
    ctx: Context,
    filters: DateFilters,
    selection: ProviderSelection,
    enable_progress: bool,
) !SummaryResult {
    return collectSummaryInternal(ctx, filters, selection, enable_progress, null, null);
}

fn collectSummaryInternal(
    ctx: Context,
    filters: DateFilters,
    selection: ProviderSelection,
    enable_progress: bool,
    session_recorder: ?*model.SessionRecorder,
    pricing_cache: ?*PricingCache,
) !SummaryResult {
    var summary_builder = model.SummaryBuilder.init(ctx.allocator);
    errdefer summary_builder.deinit(ctx.allocator);
    if (session_recorder) |recorder| summary_builder.attachSessionRecorder(recorder);

    var totals = SummaryTotals.init();
    errdefer totals.deinit(ctx.allocator);

    var progress_root: std.Progress.Node = undefined;
    const progress_parent = if (enable_progress) blk: {
        progress_root = std.Progress.start(ctx.io, .{ .root_name = "Tokenuze" });
        break :blk &progress_root;
    } else null;
    defer if (enable_progress) std.Progress.Node.end(progress_root);
    if (enable_progress) {
        errdefer std.Progress.setStatus(.failure);
    }

    var total_timer = try std.time.Timer.start();

    var temp_pricing_map: ?model.PricingMap = null;
    defer if (temp_pricing_map) |*map| model.deinitPricingMap(map, ctx.allocator);

    const pricing_map: *model.PricingMap = blk: {
        if (pricing_cache) |cache| {
            try cache.ensureLoaded(ctx.allocator, ctx.temp_allocator, selection, progress_parent);
            break :blk &cache.map;
        }

        temp_pricing_map = .init(ctx.allocator);
        if (!selection.isEmpty()) {
            try loadPricing(ctx.allocator, ctx.temp_allocator, selection, &temp_pricing_map.?, progress_parent);
        }
        break :blk &temp_pricing_map.?;
    };

    try collectSelectedProviders(ctx, filters, selection, &summary_builder, progress_parent);

    var summaries = summary_builder.items();
    if (summaries.len == 0) {
        std.log.info("no events to process; total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
        if (enable_progress) std.Progress.setStatus(.success);
        return SummaryResult{ .builder = summary_builder, .totals = totals };
    }

    try finalizeSummaries(ctx.allocator, progress_parent, summaries, pricing_map, &totals, session_recorder);

    std.log.info("phase.total runtime {d:.2}ms", .{nsToMs(total_timer.read())});
    if (enable_progress) std.Progress.setStatus(.success);

    return SummaryResult{ .builder = summary_builder, .totals = totals };
}

fn collectSelectedProviders(
    ctx: Context,
    filters: DateFilters,
    selection: ProviderSelection,
    summary_builder: *model.SummaryBuilder,
    progress_parent: ?*std.Progress.Node,
) !void {
    if (selection.isEmpty()) return;

    for (providers, 0..) |prov, idx| {
        if (!selection.includesIndex(idx)) continue;
        const before_events = summary_builder.eventCount();
        std.log.debug(
            "phase.{s} starting (events_before={d})",
            .{ prov.phase_label, before_events },
        );
        const stats = blk: {
            var collect_timer = try std.time.Timer.start();
            const phase_node = startProgressNode(progress_parent, prov.phase_label, 0);
            defer finishProgressNode(phase_node);
            try prov.collect(ctx, summary_builder, filters, progressHandle(phase_node));
            break :blk .{
                .elapsed = nsToMs(collect_timer.read()),
                .events_added = summary_builder.eventCount() - before_events,
                .total_events = summary_builder.eventCount(),
            };
        };
        std.log.info(
            "phase.{s} completed in {d:.2}ms (events += {d}, total_events={d})",
            .{
                prov.phase_label,
                stats.elapsed,
                stats.events_added,
                stats.total_events,
            },
        );
    }
}

fn finalizeSummaries(
    allocator: std.mem.Allocator,
    progress_parent: ?*std.Progress.Node,
    summaries: []DailySummary,
    pricing_map: *model.PricingMap,
    totals: *SummaryTotals,
    session_recorder: ?*model.SessionRecorder,
) !void {
    var missing_set = std.StringHashMap(u8).init(allocator);
    defer missing_set.deinit();

    const pricing_elapsed = blk: {
        var pricing_timer = try std.time.Timer.start();
        const pricing_node = startProgressNode(progress_parent, "apply pricing", summaries.len);
        defer finishProgressNode(pricing_node);
        const pricing_progress = progressHandle(pricing_node);
        for (summaries) |*summary| {
            model.applyPricing(allocator, summary, pricing_map, &missing_set);
            std.sort.pdq(ModelSummary, summary.models.items, {}, modelLessThan);
            if (pricing_progress) |node| std.Progress.Node.completeOne(node);
        }
        break :blk nsToMs(pricing_timer.read());
    };
    std.log.debug(
        "phase.apply_pricing completed in {d:.2}ms (days={d})",
        .{ pricing_elapsed, summaries.len },
    );

    if (session_recorder) |recorder| {
        recorder.applyPricing(pricing_map);
    }

    const sort_elapsed = blk: {
        var sort_timer = try std.time.Timer.start();
        const sort_node = startProgressNode(progress_parent, "sort days", 0);
        defer finishProgressNode(sort_node);
        std.sort.pdq(DailySummary, summaries, {}, summaryLessThan);
        break :blk nsToMs(sort_timer.read());
    };
    std.log.debug(
        "phase.sort_days completed in {d:.2}ms (days={d})",
        .{ sort_elapsed, summaries.len },
    );

    const totals_elapsed = blk: {
        var totals_timer = try std.time.Timer.start();
        const totals_node = startProgressNode(progress_parent, "totals", 0);
        defer finishProgressNode(totals_node);
        model.accumulateTotals(summaries, totals);
        try model.collectMissingModels(allocator, &missing_set, &totals.missing_pricing);
        break :blk nsToMs(totals_timer.read());
    };
    std.log.debug(
        "phase.totals completed in {d:.2}ms (missing_pricing={d})",
        .{ totals_elapsed, totals.missing_pricing.items.len },
    );
}

fn loadPricing(
    allocator: std.mem.Allocator,
    temp_allocator: std.mem.Allocator,
    selection: ProviderSelection,
    pricing: *model.PricingMap,
    progress_parent: ?*std.Progress.Node,
) !void {
    var pricing_timer = try std.time.Timer.start();
    const pricing_node = startProgressNode(progress_parent, "load pricing", 0);
    defer finishProgressNode(pricing_node);
    std.log.debug("pricing.load begin (selection_mask=0x{x})", .{selection.mask});

    const before_models = pricing.count();
    const remote_stats = try provider.loadRemotePricing(allocator, temp_allocator, pricing);
    if (remote_stats.attempted) {
        if (remote_stats.failure) |err| {
            std.log.warn(
                "pricing.remote_fetch failed after {d:.2}ms ({s})",
                .{ remote_stats.elapsed_ms, @errorName(err) },
            );
        } else {
            std.log.debug(
                "pricing.remote_fetch completed in {d:.2}ms (models += {d})",
                .{ remote_stats.elapsed_ms, remote_stats.models_added },
            );
        }
    } else {
        std.log.debug("pricing.remote_fetch skipped (already loaded)", .{});
    }

    var fallback_timer = try std.time.Timer.start();
    for (providers, 0..) |prov, idx| {
        if (!selection.includesIndex(idx)) continue;
        std.log.debug(
            "pricing.{s}.fallback start (models={d})",
            .{ prov.name, pricing.count() },
        );
        try prov.load_pricing(allocator, pricing);
    }
    const fallback_elapsed = nsToMs(fallback_timer.read());
    const fallback_added = pricing.count() - (before_models + remote_stats.models_added);
    std.log.debug(
        "pricing.fallback ensured in {d:.2}ms (models += {d})",
        .{ fallback_elapsed, fallback_added },
    );

    std.log.info(
        "phase.load_pricing completed in {d:.2}ms (models={d}, models_added={d})",
        .{ nsToMs(pricing_timer.read()), pricing.count(), pricing.count() - before_models },
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
    summaries: []const model.DailySummary,
    totals: *const model.SummaryTotals,
    pretty: bool,
) ![]u8 {
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);
    var writer_state = io_util.ArrayWriter.init(&buffer, allocator);
    try render.Renderer.writeJson(writer_state.writer(), summaries, totals, pretty);
    return buffer.toOwnedSlice(allocator);
}
