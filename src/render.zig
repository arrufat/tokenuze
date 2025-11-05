const std = @import("std");
const Model = @import("model.zig");

pub const Renderer = struct {
    pub fn writeSummary(
        writer: anytype,
        summaries: []const Model.DailySummary,
        totals: *const Model.SummaryTotals,
        pretty: bool,
    ) !void {
        const payload = Output{
            .days = SummaryArray{ .items = summaries },
            .total = TotalsView{ .totals = totals },
        };
        var stringify = std.json.Stringify{
            .writer = writer,
            .options = if (pretty) .{ .whitespace = .indent_2 } else .{},
        };
        try stringify.write(payload);
        try writer.writeAll("\n");
    }

    const Output = struct {
        days: SummaryArray,
        total: TotalsView,
    };

    const SummaryArray = struct {
        items: []const Model.DailySummary,

        pub fn jsonStringify(self: SummaryArray, jw: anytype) !void {
            try jw.beginArray();
            for (self.items) |*summary| {
                try jw.write(DailySummaryView{ .summary = summary });
            }
            try jw.endArray();
        }
    };

    const TotalsView = struct {
        totals: *const Model.SummaryTotals,

        pub fn jsonStringify(self: TotalsView, jw: anytype) !void {
            const totals = self.totals;
            try jw.beginObject();
            try writeUsageFields(jw, totals.usage);
            try jw.objectField("cost_usd");
            try jw.write(totals.cost_usd);
            try jw.objectField("missing_pricing");
            try jw.write(totals.missing_pricing.items);
            try jw.endObject();
        }
    };

    const DailySummaryView = struct {
        summary: *const Model.DailySummary,

        pub fn jsonStringify(self: DailySummaryView, jw: anytype) !void {
            const summary = self.summary;
            try jw.beginObject();
            try jw.objectField("date");
            try jw.write(summary.display_date);
            try jw.objectField("iso_date");
            try jw.write(summary.iso_date);
            try writeUsageFields(jw, summary.usage);
            try jw.objectField("cost_usd");
            try jw.write(summary.cost_usd);
            try jw.objectField("models");
            try jw.beginArray();
            for (summary.models.items) |*model| {
                try jw.write(ModelSummaryView{ .model = model });
            }
            try jw.endArray();
            try jw.objectField("missing_pricing");
            try jw.write(summary.missing_pricing.items);
            try jw.endObject();
        }
    };

    const ModelSummaryView = struct {
        model: *const Model.ModelSummary,

        pub fn jsonStringify(self: ModelSummaryView, jw: anytype) !void {
            const model = self.model;
            try jw.beginObject();
            try jw.objectField("name");
            try jw.write(model.name);
            try writeDisplayName(jw, model);
            try jw.objectField("is_fallback");
            try jw.write(model.is_fallback);
            try writeUsageFields(jw, model.usage);
            try jw.objectField("cost_usd");
            try jw.write(model.cost_usd);
            try jw.objectField("pricing_available");
            try jw.write(model.pricing_available);
            try jw.endObject();
        }
    };

    fn writeUsageFields(jw: anytype, usage: Model.TokenUsage) !void {
        try jw.objectField("input_tokens");
        try jw.write(usage.input_tokens);
        try jw.objectField("cache_creation_input_tokens");
        try jw.write(usage.cache_creation_input_tokens);
        try jw.objectField("cached_input_tokens");
        try jw.write(usage.cached_input_tokens);
        try jw.objectField("output_tokens");
        try jw.write(usage.output_tokens);
        try jw.objectField("reasoning_output_tokens");
        try jw.write(usage.reasoning_output_tokens);
        try jw.objectField("total_tokens");
        try jw.write(usage.total_tokens);
    }

    fn writeDisplayName(jw: anytype, model: *const Model.ModelSummary) !void {
        try jw.objectField("display_name");
        if (model.is_fallback) {
            var buffer: [256]u8 = undefined;
            const display = std.fmt.bufPrint(&buffer, "{s} (fallback)", .{model.name}) catch model.name;
            try jw.write(display);
        } else {
            try jw.write(model.name);
        }
    }
};
