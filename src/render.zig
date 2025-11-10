const std = @import("std");
const Model = @import("model.zig");

pub const Renderer = struct {
    const Alignment = enum { left, right };
    const ColumnId = enum {
        date,
        models,
        input,
        output,
        cache_create,
        cache_read,
        total_tokens,
        cost,
    };
    const Column = struct {
        id: ColumnId,
        header: []const u8,
        alignment: Alignment,
    };

    const table_columns = [_]Column{
        .{ .id = .date, .header = "Date", .alignment = .left },
        .{ .id = .models, .header = "Models", .alignment = .left },
        .{ .id = .input, .header = "Input", .alignment = .right },
        .{ .id = .output, .header = "Output", .alignment = .right },
        .{ .id = .cache_create, .header = "Cache Create", .alignment = .right },
        .{ .id = .cache_read, .header = "Cache Read", .alignment = .right },
        .{ .id = .total_tokens, .header = "Total Tokens", .alignment = .right },
        .{ .id = .cost, .header = "Cost (USD)", .alignment = .right },
    };

    const column_count = table_columns.len;
    const max_models_in_table = 3;

    const Row = struct {
        cells: [column_count][]const u8,
    };

    fn usageFieldVisibilityFromTotals(totals: *const Model.SummaryTotals) Model.UsageFieldVisibility {
        return .{
            .cache_creation = totals.usage.cache_creation_input_tokens > 0,
            .cache_read = totals.usage.cached_input_tokens > 0,
        };
    }

    fn columnUsageFromVisibility(visibility: Model.UsageFieldVisibility) [column_count]bool {
        var active = [_]bool{true} ** column_count;
        active[@intFromEnum(ColumnId.cache_create)] = visibility.cache_creation;
        active[@intFromEnum(ColumnId.cache_read)] = visibility.cache_read;
        return active;
    }

    pub fn writeJson(
        writer: *std.Io.Writer,
        summaries: []const Model.DailySummary,
        totals: *const Model.SummaryTotals,
        pretty: bool,
    ) !void {
        const field_visibility = usageFieldVisibilityFromTotals(totals);
        const payload = Output{
            .daily = SummaryArray{
                .items = summaries,
                .field_visibility = field_visibility,
            },
            .totals = TotalsView{
                .totals = totals,
                .field_visibility = field_visibility,
            },
        };
        var stringify = std.json.Stringify{
            .writer = writer,
            .options = if (pretty) .{ .whitespace = .indent_2 } else .{},
        };
        try stringify.write(payload);
        try writer.writeAll("\n");
    }

    pub fn writeTable(
        writer: *std.Io.Writer,
        allocator: std.mem.Allocator,
        summaries: []const Model.DailySummary,
        totals: *const Model.SummaryTotals,
    ) !void {
        if (summaries.len == 0) {
            try writer.writeAll("No usage data found for the selected filters.\n");
            return;
        }

        const field_visibility = usageFieldVisibilityFromTotals(totals);
        const column_usage = columnUsageFromVisibility(field_visibility);

        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var widths = [_]usize{0} ** column_count;
        for (table_columns, 0..) |column, idx| {
            if (!column_usage[idx]) continue;
            widths[idx] = column.header.len;
        }

        var rows = try arena.alloc(Row, summaries.len);
        for (summaries, 0..) |*summary, idx| {
            rows[idx] = try formatRow(arena, summary);
            updateWidths(&widths, rows[idx].cells[0..], column_usage[0..]);
        }

        const totals_row = try formatTotalsRow(arena, totals);
        updateWidths(&widths, totals_row.cells[0..], column_usage[0..]);

        try writeRule(writer, widths[0..], column_usage[0..], '-');
        var header_cells: [column_count][]const u8 = undefined;
        for (table_columns, 0..) |column, idx| {
            header_cells[idx] = column.header;
        }
        try writeRow(writer, widths[0..], header_cells[0..], table_columns[0..], column_usage[0..]);
        try writeRule(writer, widths[0..], column_usage[0..], '=');
        for (rows) |row| {
            try writeRow(writer, widths[0..], row.cells[0..], table_columns[0..], column_usage[0..]);
        }
        try writeRule(writer, widths[0..], column_usage[0..], '-');
        try writeRow(writer, widths[0..], totals_row.cells[0..], table_columns[0..], column_usage[0..]);
        try writeRule(writer, widths[0..], column_usage[0..], '-');

        if (totals.missing_pricing.items.len > 0) {
            try writer.writeAll("\nMissing pricing entries:\n");
            for (totals.missing_pricing.items) |model_name| {
                try writer.print("  - {s}\n", .{model_name});
            }
        }
    }

    const Output = struct {
        daily: SummaryArray,
        totals: TotalsView,
    };

    const SummaryArray = struct {
        items: []const Model.DailySummary,
        field_visibility: Model.UsageFieldVisibility,

        pub fn jsonStringify(self: SummaryArray, jw: *std.json.Stringify) !void {
            try jw.beginArray();
            for (self.items) |*summary| {
                try jw.write(DailySummaryView{
                    .summary = summary,
                    .field_visibility = self.field_visibility,
                });
            }
            try jw.endArray();
        }
    };

    const TotalsView = struct {
        totals: *const Model.SummaryTotals,
        field_visibility: Model.UsageFieldVisibility,

        pub fn jsonStringify(self: TotalsView, jw: *std.json.Stringify) !void {
            const totals = self.totals;
            try jw.beginObject();
            try Model.writeUsageJsonFields(jw, totals.usage, totals.display_input_tokens, self.field_visibility);
            try jw.objectField("costUSD");
            try jw.write(totals.cost_usd);
            try jw.objectField("missingPricing");
            try jw.write(totals.missing_pricing.items);
            try jw.endObject();
        }
    };

    const DailySummaryView = struct {
        summary: *const Model.DailySummary,
        field_visibility: Model.UsageFieldVisibility,

        pub fn jsonStringify(self: DailySummaryView, jw: *std.json.Stringify) !void {
            const summary = self.summary;
            try jw.beginObject();
            try jw.objectField("date");
            try jw.write(summary.display_date);
            try jw.objectField("isoDate");
            try jw.write(summary.iso_date);
            try Model.writeUsageJsonFields(jw, summary.usage, summary.display_input_tokens, self.field_visibility);
            try jw.objectField("costUSD");
            try jw.write(summary.cost_usd);
            try jw.objectField("models");
            try jw.write(ModelMapView{
                .models = summary.models.items,
                .field_visibility = self.field_visibility,
            });
            try jw.objectField("missingPricing");
            try jw.write(summary.missing_pricing.items);
            try jw.endObject();
        }
    };

    const ModelMapView = struct {
        models: []const Model.ModelSummary,
        field_visibility: Model.UsageFieldVisibility,

        pub fn jsonStringify(self: ModelMapView, jw: *std.json.Stringify) !void {
            try jw.beginObject();
            for (self.models) |*model| {
                try jw.objectField(model.name);
                try jw.beginObject();
                try Model.writeUsageJsonFields(jw, model.usage, model.display_input_tokens, self.field_visibility);
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

    fn formatRow(allocator: std.mem.Allocator, summary: *const Model.DailySummary) !Row {
        var cells: [column_count][]const u8 = undefined;
        cells[0] = summary.display_date;
        cells[1] = try formatModels(allocator, summary.models.items);
        try formatUsageCells(allocator, summary, &cells);
        return Row{ .cells = cells };
    }

    fn formatTotalsRow(allocator: std.mem.Allocator, totals: *const Model.SummaryTotals) !Row {
        var cells: [column_count][]const u8 = undefined;
        cells[0] = "TOTAL";
        cells[1] = "-";
        try formatUsageCells(allocator, totals, &cells);
        return Row{ .cells = cells };
    }

    fn formatUsageCells(
        allocator: std.mem.Allocator,
        data: anytype,
        cells: *[column_count][]const u8,
    ) !void {
        const value = switch (@typeInfo(@TypeOf(data))) {
            .pointer => data.*,
            else => data,
        };
        const input_tokens = effectiveInputTokens(value.usage, value.display_input_tokens);
        cells[2] = try formatNumber(allocator, input_tokens);
        cells[3] = try formatNumber(allocator, value.usage.output_tokens);
        cells[4] = try formatNumber(allocator, value.usage.cache_creation_input_tokens);
        cells[5] = try formatNumber(allocator, value.usage.cached_input_tokens);
        cells[6] = try formatNumber(allocator, value.usage.total_tokens);
        cells[7] = try formatCurrency(allocator, value.cost_usd);
    }

    fn effectiveInputTokens(usage: Model.TokenUsage, display_override: u64) u64 {
        if (display_override > 0) return display_override;
        return usage.input_tokens;
    }

    fn updateWidths(
        widths: *[column_count]usize,
        cells: []const []const u8,
        column_usage: []const bool,
    ) void {
        for (cells, 0..) |cell, idx| {
            if (!column_usage[idx]) continue;
            if (cell.len > widths[idx]) widths[idx] = cell.len;
        }
    }

    fn writeRule(
        writer: *std.Io.Writer,
        widths: []const usize,
        column_usage: []const bool,
        ch: u8,
    ) !void {
        try writer.writeAll("+");
        for (widths, 0..) |width, idx| {
            if (!column_usage[idx]) continue;
            try writer.splatByteAll(ch, width + 2);
            try writer.writeAll("+");
        }
        try writer.writeAll("\n");
    }

    fn writeRow(
        writer: *std.Io.Writer,
        widths: []const usize,
        cells: []const []const u8,
        columns: []const Column,
        column_usage: []const bool,
    ) !void {
        try writer.writeAll("|");
        for (cells, 0..) |cell, idx| {
            if (!column_usage[idx]) continue;
            const width = widths[idx];
            const alignment = columns[idx].alignment;
            const padding = if (width > cell.len) width - cell.len else 0;
            try writer.writeAll(" ");
            switch (alignment) {
                .left => {
                    try writer.writeAll(cell);
                    try writer.splatByteAll(' ', padding);
                },
                .right => {
                    try writer.splatByteAll(' ', padding);
                    try writer.writeAll(cell);
                },
            }
            try writer.writeAll(" ");
            try writer.writeAll("|");
        }
        try writer.writeAll("\n");
    }

    fn formatModels(
        allocator: std.mem.Allocator,
        models: []const Model.ModelSummary,
    ) ![]const u8 {
        if (models.len == 0) {
            return allocator.dupe(u8, "-");
        }
        var buffer = std.ArrayList(u8).empty;
        errdefer buffer.deinit(allocator);
        const display_count = if (models.len < max_models_in_table) models.len else max_models_in_table;
        for (models[0..display_count], 0..) |model, idx| {
            if (idx > 0) try buffer.appendSlice(allocator, ", ");
            try buffer.appendSlice(allocator, model.name);
        }
        if (models.len > max_models_in_table) {
            var suffix_buf: [32]u8 = undefined;
            const suffix = try std.fmt.bufPrint(&suffix_buf, " (+{d} more)", .{models.len - max_models_in_table});
            try buffer.appendSlice(allocator, suffix);
        }
        return buffer.toOwnedSlice(allocator);
    }

    fn formatNumber(allocator: std.mem.Allocator, value: u64) ![]const u8 {
        var tmp: [32]u8 = undefined;
        const digits = try std.fmt.bufPrint(&tmp, "{d}", .{value});
        return try formatDigitsWithCommas(allocator, digits);
    }

    fn formatCurrency(allocator: std.mem.Allocator, amount: f64) ![]const u8 {
        const negative = amount < 0;
        const magnitude = @abs(amount);
        var tmp: [64]u8 = undefined;
        const raw = try std.fmt.bufPrint(&tmp, "{d:.2}", .{magnitude});
        const dot_index = std.mem.indexOfScalar(u8, raw, '.') orelse raw.len;
        const integer = raw[0..dot_index];
        const decimals = if (dot_index < raw.len) raw[dot_index..] else "";
        const comma_integer = try formatDigitsWithCommas(allocator, integer);
        defer allocator.free(comma_integer);
        return try std.fmt.allocPrint(allocator, "{s}${s}{s}", .{
            if (negative) "-" else "",
            comma_integer,
            decimals,
        });
    }

    fn formatDigitsWithCommas(
        allocator: std.mem.Allocator,
        digits: []const u8,
    ) ![]const u8 {
        if (digits.len <= 3) {
            return allocator.dupe(u8, digits);
        }
        const comma_count = (digits.len - 1) / 3;
        const total_len = digits.len + comma_count;
        var result = try allocator.alloc(u8, total_len);
        var src_index = digits.len;
        var dst_index = total_len;
        var group_len: usize = 0;
        while (src_index > 0) {
            src_index -= 1;
            dst_index -= 1;
            result[dst_index] = digits[src_index];
            group_len += 1;
            if (group_len == 3 and src_index > 0) {
                dst_index -= 1;
                result[dst_index] = ',';
                group_len = 0;
            }
        }
        return result;
    }
};
