const std = @import("std");
const model = @import("model.zig");

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
        header: []const u8,
        alignment: Alignment,
    };

    const table_columns = [_]Column{
        .{ .header = "Date", .alignment = .left },
        .{ .header = "Models", .alignment = .left },
        .{ .header = "Input", .alignment = .right },
        .{ .header = "Output", .alignment = .right },
        .{ .header = "Cache Create", .alignment = .right },
        .{ .header = "Cache Read", .alignment = .right },
        .{ .header = "Total Tokens", .alignment = .right },
        .{ .header = "Cost (USD)", .alignment = .right },
    };
    comptime {
        const fields = @typeInfo(ColumnId).@"enum".fields;
        if (table_columns.len != fields.len)
            @compileError("table_columns must align with ColumnId ordering");
    }

    const column_count = table_columns.len;
    const max_models_in_table = 3;

    const SessionColumnId = enum {
        session,
        activity,
        models,
        input,
        output,
        cache_create,
        cache_read,
        reasoning,
        total_tokens,
        cost,
    };

    const session_columns = [_]Column{
        .{ .header = "Session", .alignment = .left },
        .{ .header = "Last Activity", .alignment = .left },
        .{ .header = "Models", .alignment = .left },
        .{ .header = "Input", .alignment = .right },
        .{ .header = "Output", .alignment = .right },
        .{ .header = "Cache Create", .alignment = .right },
        .{ .header = "Cache Read", .alignment = .right },
        .{ .header = "Reasoning", .alignment = .right },
        .{ .header = "Total Tokens", .alignment = .right },
        .{ .header = "Cost (USD)", .alignment = .right },
    };
    comptime {
        const fields = @typeInfo(SessionColumnId).@"enum".fields;
        if (session_columns.len != fields.len)
            @compileError("session_columns must align with SessionColumnId ordering");
    }

    const session_column_count = session_columns.len;

    const Row = struct {
        cells: [column_count][]const u8,
    };

    const SessionRow = struct {
        cells: [session_column_count][]const u8,
    };

    fn usageFieldVisibilityFromTotals(totals: *const model.SummaryTotals) model.UsageFieldVisibility {
        return .{
            .cache_creation = totals.usage.cache_creation_input_tokens > 0,
            .cache_read = totals.usage.cached_input_tokens > 0,
        };
    }

    fn usageFieldVisibilityFromTokenUsage(usage: model.TokenUsage) model.UsageFieldVisibility {
        return .{
            .cache_creation = usage.cache_creation_input_tokens > 0,
            .cache_read = usage.cached_input_tokens > 0,
        };
    }

    fn columnUsageFromVisibility(visibility: model.UsageFieldVisibility) [column_count]bool {
        var active = [_]bool{true} ** column_count;
        active[@intFromEnum(ColumnId.cache_create)] = visibility.cache_creation;
        active[@intFromEnum(ColumnId.cache_read)] = visibility.cache_read;
        return active;
    }

    pub fn writeJson(
        writer: *std.Io.Writer,
        summaries: []const model.DailySummary,
        totals: *const model.SummaryTotals,
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
        summaries: []const model.DailySummary,
        totals: *const model.SummaryTotals,
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
            updateWidths(widths[0..], rows[idx].cells[0..], column_usage[0..]);
        }

        const totals_row = try formatTotalsRow(arena, totals);
        updateWidths(widths[0..], totals_row.cells[0..], column_usage[0..]);

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

    pub fn writeSessionsTable(
        writer: *std.Io.Writer,
        allocator: std.mem.Allocator,
        recorder: *const model.SessionRecorder,
    ) !void {
        var sessions = try recorder.sortedSessions(allocator);
        defer sessions.deinit(allocator);

        if (sessions.items.len == 0) {
            try writer.writeAll("No session data found for the selected filters.\n");
            return;
        }

        const visibility = usageFieldVisibilityFromTokenUsage(recorder.totals);
        const show_reasoning = recorder.totals.reasoning_output_tokens > 0;
        var column_usage = [_]bool{true} ** session_column_count;
        column_usage[@intFromEnum(SessionColumnId.cache_create)] = visibility.cache_creation;
        column_usage[@intFromEnum(SessionColumnId.cache_read)] = visibility.cache_read;
        column_usage[@intFromEnum(SessionColumnId.reasoning)] = show_reasoning;

        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var widths = [_]usize{0} ** session_column_count;
        for (session_columns, 0..) |column, idx| {
            if (!column_usage[idx]) continue;
            widths[idx] = column.header.len;
        }

        var rows = try arena.alloc(SessionRow, sessions.items.len);
        for (sessions.items, 0..) |session, idx| {
            rows[idx] = try formatSessionRow(arena, session);
            updateWidths(widths[0..], rows[idx].cells[0..], column_usage[0..]);
        }

        const totals_row = try formatSessionTotalsRow(arena, recorder, sessions.items.len);
        updateWidths(widths[0..], totals_row.cells[0..], column_usage[0..]);

        try writeRule(writer, widths[0..], column_usage[0..], '-');
        var header_cells: [session_column_count][]const u8 = undefined;
        for (session_columns, 0..) |column, idx| {
            header_cells[idx] = column.header;
        }
        try writeRow(writer, widths[0..], header_cells[0..], session_columns[0..], column_usage[0..]);
        try writeRule(writer, widths[0..], column_usage[0..], '=');
        for (rows) |row| {
            try writeRow(writer, widths[0..], row.cells[0..], session_columns[0..], column_usage[0..]);
        }
        try writeRule(writer, widths[0..], column_usage[0..], '-');
        try writeRow(writer, widths[0..], totals_row.cells[0..], session_columns[0..], column_usage[0..]);
        try writeRule(writer, widths[0..], column_usage[0..], '-');
    }

    const Output = struct {
        daily: SummaryArray,
        totals: TotalsView,
    };

    const SummaryArray = struct {
        items: []const model.DailySummary,
        field_visibility: model.UsageFieldVisibility,

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
        totals: *const model.SummaryTotals,
        field_visibility: model.UsageFieldVisibility,

        pub fn jsonStringify(self: TotalsView, jw: *std.json.Stringify) !void {
            const totals = self.totals;
            try jw.beginObject();
            try model.writeUsageJsonFields(jw, totals.usage, totals.display_input_tokens, self.field_visibility);
            try jw.objectField("costUSD");
            try jw.write(totals.cost_usd);
            try jw.objectField("missingPricing");
            try jw.write(totals.missing_pricing.items);
            try jw.endObject();
        }
    };

    const DailySummaryView = struct {
        summary: *const model.DailySummary,
        field_visibility: model.UsageFieldVisibility,

        pub fn jsonStringify(self: DailySummaryView, jw: *std.json.Stringify) !void {
            const summary = self.summary;
            try jw.beginObject();
            try jw.objectField("date");
            try jw.write(summary.display_date);
            try jw.objectField("isoDate");
            try jw.write(summary.iso_date);
            try model.writeUsageJsonFields(jw, summary.usage, summary.display_input_tokens, self.field_visibility);
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
        models: []const model.ModelSummary,
        field_visibility: model.UsageFieldVisibility,

        pub fn jsonStringify(self: ModelMapView, jw: *std.json.Stringify) !void {
            try jw.beginObject();
            for (self.models) |*mod| {
                try jw.objectField(mod.name);
                try jw.beginObject();
                try model.writeUsageJsonFields(jw, mod.usage, mod.display_input_tokens, self.field_visibility);
                try jw.objectField("costUSD");
                try jw.write(mod.cost_usd);
                try jw.objectField("pricingAvailable");
                try jw.write(mod.pricing_available);
                try jw.objectField("isFallback");
                try jw.write(mod.is_fallback);
                try jw.endObject();
            }
            try jw.endObject();
        }
    };

    fn formatSessionRow(
        allocator: std.mem.Allocator,
        session: *const model.SessionRecorder.SessionEntry,
    ) !SessionRow {
        var cells: [session_column_count][]const u8 = undefined;
        cells[@intFromEnum(SessionColumnId.session)] = session.session_id;
        cells[@intFromEnum(SessionColumnId.activity)] = session.last_activity orelse "-";
        cells[@intFromEnum(SessionColumnId.models)] = try formatSessionModels(allocator, session.models.items);
        cells[@intFromEnum(SessionColumnId.input)] = try formatNumber(allocator, session.usage.input_tokens);
        cells[@intFromEnum(SessionColumnId.output)] = try formatNumber(allocator, session.usage.output_tokens);
        cells[@intFromEnum(SessionColumnId.cache_create)] = try formatNumber(allocator, session.usage.cache_creation_input_tokens);
        cells[@intFromEnum(SessionColumnId.cache_read)] = try formatNumber(allocator, session.usage.cached_input_tokens);
        cells[@intFromEnum(SessionColumnId.reasoning)] = try formatNumber(allocator, session.usage.reasoning_output_tokens);
        cells[@intFromEnum(SessionColumnId.total_tokens)] = try formatNumber(allocator, session.usage.total_tokens);
        cells[@intFromEnum(SessionColumnId.cost)] = try formatCurrency(allocator, session.cost_usd);
        return SessionRow{ .cells = cells };
    }

    fn formatSessionTotalsRow(
        allocator: std.mem.Allocator,
        recorder: *const model.SessionRecorder,
        session_count: usize,
    ) !SessionRow {
        var cells: [session_column_count][]const u8 = undefined;
        cells[@intFromEnum(SessionColumnId.session)] = "TOTAL";
        cells[@intFromEnum(SessionColumnId.activity)] = "-";
        cells[@intFromEnum(SessionColumnId.models)] = try std.fmt.allocPrint(
            allocator,
            "{d} sessions",
            .{session_count},
        );
        const display_input = effectiveInputTokens(recorder.totals, recorder.display_total_input_tokens);
        cells[@intFromEnum(SessionColumnId.input)] = try formatNumber(allocator, display_input);
        cells[@intFromEnum(SessionColumnId.output)] = try formatNumber(allocator, recorder.totals.output_tokens);
        cells[@intFromEnum(SessionColumnId.cache_create)] = try formatNumber(allocator, recorder.totals.cache_creation_input_tokens);
        cells[@intFromEnum(SessionColumnId.cache_read)] = try formatNumber(allocator, recorder.totals.cached_input_tokens);
        cells[@intFromEnum(SessionColumnId.reasoning)] = try formatNumber(allocator, recorder.totals.reasoning_output_tokens);
        cells[@intFromEnum(SessionColumnId.total_tokens)] = try formatNumber(allocator, recorder.totals.total_tokens);
        cells[@intFromEnum(SessionColumnId.cost)] = try formatCurrency(allocator, recorder.total_cost_usd);
        return SessionRow{ .cells = cells };
    }

    fn formatSessionModels(
        allocator: std.mem.Allocator,
        models: []const model.SessionRecorder.SessionModel,
    ) ![]const u8 {
        if (models.len == 0) return "-";
        const count = @min(models.len, max_models_in_table);
        const names = try allocator.alloc([]const u8, count);
        for (names, 0..) |*slot, idx| {
            slot.* = models[idx].name;
        }
        var joined = try std.mem.join(allocator, ", ", names);
        allocator.free(names);
        if (models.len > count) {
            const old_joined = joined;
            joined = try std.fmt.allocPrint(allocator, "{s} (+{d} more)", .{ old_joined, models.len - count });
            allocator.free(old_joined);
        }
        return joined;
    }

    fn formatRow(allocator: std.mem.Allocator, summary: *const model.DailySummary) !Row {
        var cells: [column_count][]const u8 = undefined;
        cells[0] = summary.display_date;
        cells[1] = try formatModels(allocator, summary.models.items);
        try formatUsageCells(allocator, summary, &cells);
        return Row{ .cells = cells };
    }

    fn formatTotalsRow(allocator: std.mem.Allocator, totals: *const model.SummaryTotals) !Row {
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

    fn effectiveInputTokens(usage: model.TokenUsage, display_override: u64) u64 {
        if (display_override > 0) return display_override;
        return usage.input_tokens;
    }

    fn updateWidths(widths: []usize, cells: []const []const u8, column_usage: []const bool) void {
        for (cells, 0..) |cell, idx| {
            if (!column_usage[idx]) continue;
            if (cell.len > widths[idx]) widths[idx] = cell.len;
        }
    }

    fn writeRule(writer: anytype, widths: []const usize, column_usage: []const bool, ch: u8) !void {
        try writer.writeAll("+");
        for (widths, 0..) |width, idx| {
            if (!column_usage[idx]) continue;
            try writer.splatByteAll(ch, width + 2);
            try writer.writeAll("+");
        }
        try writer.writeAll("\n");
    }

    fn writeRow(
        writer: anytype,
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
        models: []const model.ModelSummary,
    ) ![]const u8 {
        if (models.len == 0) {
            return allocator.dupe(u8, "-");
        }
        var buffer = std.ArrayList(u8).empty;
        errdefer buffer.deinit(allocator);
        const display_count = if (models.len < max_models_in_table) models.len else max_models_in_table;
        for (models[0..display_count], 0..) |mod, idx| {
            if (idx > 0) try buffer.appendSlice(allocator, ", ");
            try buffer.appendSlice(allocator, mod.name);
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
        const dot_index = std.mem.findScalar(u8, raw, '.') orelse raw.len;
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

    test "writeSessionsTable renders tiny snapshot" {
        const allocator = std.testing.allocator;
        var recorder = model.SessionRecorder.init(allocator);
        defer recorder.deinit(allocator);

        // Manually insert one session with simple usage and cost
        var gop = try recorder.sessions.getOrPut("s1");
        gop.key_ptr.* = try allocator.dupe(u8, "s1");
        gop.value_ptr.* = model.SessionRecorder.SessionEntry.init("s1", "", "");
        gop.value_ptr.usage.input_tokens = 10;
        gop.value_ptr.usage.total_tokens = 10;
        gop.value_ptr.cost_usd = 1.23;
        recorder.totals.input_tokens = 10;
        recorder.totals.total_tokens = 10;
        recorder.total_cost_usd = 1.23;

        var buffer: [256]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        const writer = &stream.writer();

        try Renderer.writeSessionsTable(writer, allocator, &recorder);
        const out = stream.getWritten();
        try std.testing.expect(std.mem.indexOf(u8, out, "s1") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "$1.23") != null);
    }

    test "formatDigitsWithCommas works" {
        const allocator = std.testing.allocator;
        const input = "1234567".*;
        const output = try formatDigitsWithCommas(allocator, input);
        defer allocator.free(output);
        try std.testing.expectEqualStrings("1,234,567", output);
    }

    test "writeRow respects alignment and widths" {
        var list = std.ArrayListUnmanaged(u8){};
        defer list.deinit(std.testing.allocator);

        const columns = [_]Column{
            .{ .header = "Left", .alignment = .left },
            .{ .header = "Right", .alignment = .right },
        };
        var widths = [_]usize{ 4, 5 };
        const cells = [_][]const u8{ "a", "b" };
        const usage = [_]bool{ true, true };

        const TestWriter = struct {
            list: *std.ArrayListUnmanaged(u8),
            alloc: std.mem.Allocator,

            pub fn writeAll(self: *@This(), bytes: []const u8) !void {
                try self.list.appendSlice(self.alloc, bytes);
            }

            pub fn splatByteAll(self: *@This(), byte: u8, count: usize) !void {
                try self.list.appendNTimes(self.alloc, byte, count);
            }
        };

        var writer = TestWriter{ .list = &list, .alloc = std.testing.allocator };
        try writeRow(&writer, &widths, &cells, &columns, &usage);
        try std.testing.expectEqualStrings("| a    |     b |\n", list.items);
    }
};
