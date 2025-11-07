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
            .daily = SummaryArray{ .items = summaries },
            .totals = TotalsView{ .totals = totals },
        };
        var stringify = std.json.Stringify{
            .writer = writer,
            .options = if (pretty) .{ .whitespace = .indent_2 } else .{},
        };
        try stringify.write(payload);
        try writer.writeAll("\n");
    }

    const Output = struct {
        daily: SummaryArray,
        totals: TotalsView,
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
            try Model.writeUsageJsonFields(jw, totals.usage, totals.display_input_tokens);
            try jw.objectField("costUSD");
            try jw.write(totals.cost_usd);
            try jw.objectField("missingPricing");
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
            try jw.objectField("isoDate");
            try jw.write(summary.iso_date);
            try Model.writeUsageJsonFields(jw, summary.usage, summary.display_input_tokens);
            try jw.objectField("costUSD");
            try jw.write(summary.cost_usd);
            try jw.objectField("models");
            try jw.write(ModelMapView{ .models = summary.models.items });
            try jw.objectField("missingPricing");
            try jw.write(summary.missing_pricing.items);
            try jw.endObject();
        }
    };

    const ModelMapView = struct {
        models: []const Model.ModelSummary,

        pub fn jsonStringify(self: ModelMapView, jw: anytype) !void {
            try jw.beginObject();
            for (self.models) |*model| {
                try jw.objectField(model.name);
                try jw.beginObject();
                try Model.writeUsageJsonFields(jw, model.usage, model.display_input_tokens);
                try jw.objectField("costUSD");
                try jw.write(model.cost_usd);
                try jw.objectField("pricingAvailable");
                try jw.write(model.pricing_available);
                try jw.objectField("isFallback");
                try jw.write(model.is_fallback);
                try jw.endObject();
            }
            try jw.endObject();
        }
    };
};
