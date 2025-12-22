const std = @import("std");
const windows = std.os.windows;
const builtin = @import("builtin");

const c = @cImport({
    @cInclude("time.h");
});
const win = if (builtin.target.os.tag == .windows)
    @cImport({
        @cInclude("windows.h");
    })
else
    struct {};

pub const default_timezone_offset_minutes: i32 = 0;
// Sentinel value to indicate "use system local timezone rules (including DST) per timestamp".
pub const local_timezone_sentinel: i32 = std.math.minInt(i16);

pub const TimestampError = error{
    InvalidFormat,
    InvalidDate,
    InvalidTimeZone,
    OutOfRange,
    OutOfMemory,
};

pub const ParseTimezoneError = error{
    InvalidFormat,
    OutOfRange,
};

const seconds_per_day: i64 = 24 * 60 * 60;

pub fn isoDateForTimezone(timestamp: []const u8, offset_minutes: i32) TimestampError![10]u8 {
    if (offset_minutes == local_timezone_sentinel) {
        return isoDateForLocalTimezone(timestamp);
    }
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    return utcSecondsToOffsetIsoDate(utc_seconds, offset_minutes);
}

pub fn formatTimestampForTimezone(
    allocator: std.mem.Allocator,
    timestamp: []const u8,
    offset_minutes: i32,
) TimestampError![]u8 {
    if (offset_minutes == local_timezone_sentinel) {
        return formatTimestampForLocalTimezone(allocator, timestamp);
    }
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    const local_seconds_i64 = utc_seconds + (@as(i64, offset_minutes) * 60);
    const local_seconds = std.math.cast(u64, local_seconds_i64) orelse return error.OutOfRange;

    const epoch = std.time.epoch.EpochSeconds{ .secs = local_seconds };
    const epoch_day = epoch.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch.getDaySeconds();

    var tz_buf: [16]u8 = undefined;
    const tz_label = formatTimezoneLabel(&tz_buf, offset_minutes);

    return std.fmt.allocPrint(
        allocator,
        "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2} {s}",
        .{
            year_day.year,
            month_day.month.numeric(),
            @as(u8, month_day.day_index) + 1,
            day_seconds.getHoursIntoDay(),
            day_seconds.getMinutesIntoHour(),
            day_seconds.getSecondsIntoMinute(),
            tz_label,
        },
    );
}

pub fn parseTimezoneOffsetMinutes(input: []const u8) ParseTimezoneError!i32 {
    const trimmed = std.mem.trim(u8, input, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidFormat;

    var remaining = trimmed;
    if (remaining.len >= 3 and std.ascii.eqlIgnoreCase(remaining[0..3], "utc")) {
        remaining = remaining[3..];
        remaining = std.mem.trimStart(u8, remaining, " \t");
        if (remaining.len == 0) return 0;
    }

    var sign: i32 = 1;
    switch (remaining[0]) {
        '+' => remaining = remaining[1..],
        '-' => {
            sign = -1;
            remaining = remaining[1..];
        },
        'Z', 'z' => {
            if (remaining.len != 1) return error.InvalidFormat;
            return 0;
        },
        else => {},
    }

    if (remaining.len == 0) return error.InvalidFormat;

    var hours: i32 = 0;
    var minutes: i32 = 0;

    if (std.mem.findScalar(u8, remaining, ':')) |colon_idx| {
        const hours_part = remaining[0..colon_idx];
        const minutes_part = remaining[colon_idx + 1 ..];
        if (hours_part.len == 0 or hours_part.len > 2) return error.InvalidFormat;
        if (minutes_part.len != 2) return error.InvalidFormat;
        hours = std.fmt.parseInt(i32, hours_part, 10) catch return error.InvalidFormat;
        minutes = std.fmt.parseInt(i32, minutes_part, 10) catch return error.InvalidFormat;
    } else {
        switch (remaining.len) {
            1, 2 => {
                hours = std.fmt.parseInt(i32, remaining, 10) catch return error.InvalidFormat;
            },
            4 => {
                hours = std.fmt.parseInt(i32, remaining[0..2], 10) catch return error.InvalidFormat;
                minutes = std.fmt.parseInt(i32, remaining[2..4], 10) catch return error.InvalidFormat;
            },
            else => return error.InvalidFormat,
        }
    }

    if (minutes >= 60) return error.InvalidFormat;

    const total_minutes = hours * 60 + minutes;
    const offset = sign * total_minutes;
    if (offset < -12 * 60 or offset > 14 * 60) return error.OutOfRange;
    return offset;
}

pub fn currentTimestampIso8601(allocator: std.mem.Allocator) ![]u8 {
    const secs = try currentUnixSeconds();
    const epoch = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = epoch.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch.getDaySeconds();
    return std.fmt.allocPrint(
        allocator,
        "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z",
        .{
            year_day.year,
            month_day.month.numeric(),
            @as(u8, month_day.day_index) + 1,
            day_seconds.getHoursIntoDay(),
            day_seconds.getMinutesIntoHour(),
            day_seconds.getSecondsIntoMinute(),
        },
    );
}

pub fn currentUnixSeconds() !u64 {
    return switch (builtin.target.os.tag) {
        .windows => windowsUnixSeconds(),
        else => posixUnixSeconds(),
    };
}

pub const TimezoneError = error{TimezoneUnavailable};

pub fn detectLocalTimezoneLabel(allocator: std.mem.Allocator) ![]u8 {
    const offset = detectLocalTimezoneOffsetMinutes() catch return allocator.dupe(u8, "UTC+00:00");
    return formatTimezoneLabelAlloc(allocator, offset);
}

pub fn detectLocalTimezoneOffsetMinutes() !i32 {
    return switch (builtin.target.os.tag) {
        .windows => detectWindowsTimezoneMinutes(),
        .wasi => 0,
        else => detectPosixTimezoneMinutes(),
    };
}

pub fn formatTimezoneLabel(buffer: *[16]u8, offset_minutes: i32) []const u8 {
    const resolved = if (offset_minutes == local_timezone_sentinel)
        (detectLocalTimezoneOffsetMinutes() catch default_timezone_offset_minutes)
    else
        offset_minutes;
    const clamped = std.math.clamp(resolved, -12 * 60, 14 * 60);
    const sign: u8 = if (clamped >= 0) '+' else '-';
    const abs_minutes = @abs(clamped);
    const hours = abs_minutes / 60;
    const mins = abs_minutes % 60;
    return std.fmt.bufPrint(buffer, "UTC{c}{d:0>2}:{d:0>2}", .{ sign, hours, mins }) catch unreachable;
}

fn isoDateForLocalTimezone(timestamp: []const u8) TimestampError![10]u8 {
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    if (utc_seconds < 0) return error.OutOfRange;
    const TimeT = c.time_t;
    const casted = std.math.cast(TimeT, utc_seconds) orelse return error.OutOfRange;
    var t_value: TimeT = casted;
    var local_tm: c.tm = undefined;
    try localtimeSafe(&t_value, &local_tm);

    const year = @as(i32, local_tm.tm_year) + 1900;
    if (year < 0 or year > 9999) return error.OutOfRange;
    const month = @as(u8, @intCast(local_tm.tm_mon + 1));
    const day = @as(u8, @intCast(local_tm.tm_mday));

    var buffer: [10]u8 = undefined;
    writeFourDigits(@intCast(year), buffer[0..4]);
    buffer[4] = '-';
    writeTwoDigits(month, buffer[5..7]);
    buffer[7] = '-';
    writeTwoDigits(day, buffer[8..10]);
    return buffer;
}

fn formatTimestampForLocalTimezone(allocator: std.mem.Allocator, timestamp: []const u8) TimestampError![]u8 {
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    if (utc_seconds < 0) return error.OutOfRange;
    const TimeT = c.time_t;
    const casted = std.math.cast(TimeT, utc_seconds) orelse return error.OutOfRange;
    var t_value: TimeT = casted;
    var local_tm: c.tm = undefined;
    try localtimeSafe(&t_value, &local_tm);

    const year = @as(i32, local_tm.tm_year) + 1900;
    if (year < 0 or year > 9999) return error.OutOfRange;
    const month = @as(u8, @intCast(local_tm.tm_mon + 1));
    const day = @as(u8, @intCast(local_tm.tm_mday));

    const tz_minutes = if (@hasField(c.tm, "tm_gmtoff"))
        @as(i32, @intCast(@divTrunc(@field(local_tm, "tm_gmtoff"), 60)))
    else if (@hasField(c.tm, "__tm_gmtoff"))
        @as(i32, @intCast(@divTrunc(@field(local_tm, "__tm_gmtoff"), 60)))
    else blk: {
        var utc_tm: c.tm = undefined;
        gmtimeSafe(&t_value, &utc_tm) catch break :blk default_timezone_offset_minutes;
        const local_secs = tmToUnixSeconds(local_tm);
        const utc_secs = tmToUnixSeconds(utc_tm);
        const delta = local_secs - utc_secs;
        break :blk @as(i32, @intCast(@divTrunc(delta, 60)));
    };

    var tz_buf: [16]u8 = undefined;
    const tz_label = formatTimezoneLabel(&tz_buf, tz_minutes);

    return std.fmt.allocPrint(
        allocator,
        "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2} {s}",
        .{
            year,
            month,
            day,
            @as(u8, @intCast(local_tm.tm_hour)),
            @as(u8, @intCast(local_tm.tm_min)),
            @as(u8, @intCast(local_tm.tm_sec)),
            tz_label,
        },
    );
}

pub fn formatTimezoneLabelAlloc(allocator: std.mem.Allocator, offset_minutes: i32) ![]u8 {
    var buffer: [16]u8 = undefined;
    const label = formatTimezoneLabel(&buffer, offset_minutes);
    return allocator.dupe(u8, label);
}

pub fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

test "isoDateForTimezone adjusts across day boundaries" {
    const positive = try isoDateForTimezone("2025-09-01T16:30:00Z", 9 * 60);
    try std.testing.expectEqualStrings("2025-09-02", positive[0..]);

    const negative = try isoDateForTimezone("2025-09-01T04:00:00Z", -5 * 60);
    try std.testing.expectEqualStrings("2025-08-31", negative[0..]);
}

test "formatTimestampForTimezone shifts and labels" {
    const allocator = std.testing.allocator;
    const formatted = try formatTimestampForTimezone(allocator, "2025-02-15T18:30:00Z", -5 * 60);
    defer allocator.free(formatted);
    try std.testing.expectEqualStrings("2025-02-15 13:30:00 UTC-05:00", formatted);
}

test "parseTimezoneOffsetMinutes handles various formats" {
    try std.testing.expectEqual(@as(i32, 0), try parseTimezoneOffsetMinutes("Z"));
    try std.testing.expectEqual(@as(i32, 0), try parseTimezoneOffsetMinutes("UTC"));
    try std.testing.expectEqual(@as(i32, 330), try parseTimezoneOffsetMinutes("+05:30"));
    try std.testing.expectEqual(@as(i32, -330), try parseTimezoneOffsetMinutes("-05:30"));
    try std.testing.expectEqual(@as(i32, 345), try parseTimezoneOffsetMinutes("UTC+05:45"));
    try std.testing.expectEqual(@as(i32, 720), try parseTimezoneOffsetMinutes("+12"));
    try std.testing.expectEqual(@as(i32, -720), try parseTimezoneOffsetMinutes("-12"));
    try std.testing.expectEqual(@as(i32, 90), try parseTimezoneOffsetMinutes("+0130"));
    try std.testing.expectError(error.InvalidFormat, parseTimezoneOffsetMinutes("invalid"));
    try std.testing.expectEqual(@as(i32, 600), try parseTimezoneOffsetMinutes("UTC10"));
    try std.testing.expectError(error.InvalidFormat, parseTimezoneOffsetMinutes("+1:3"));
    try std.testing.expectError(error.OutOfRange, parseTimezoneOffsetMinutes("+15"));
}

fn parseIso8601ToUtcSeconds(timestamp: []const u8) TimestampError!i64 {
    const split_index = std.mem.findScalar(u8, timestamp, 'T') orelse return error.InvalidFormat;
    const date_part = timestamp[0..split_index];
    const time_part = timestamp[split_index + 1 ..];

    if (date_part.len != 10) return error.InvalidFormat;
    if (date_part[4] != '-' or date_part[7] != '-') return error.InvalidFormat;

    const year = std.fmt.parseInt(u16, date_part[0..4], 10) catch return error.InvalidFormat;
    const month = std.fmt.parseInt(u8, date_part[5..7], 10) catch return error.InvalidFormat;
    const day = std.fmt.parseInt(u8, date_part[8..10], 10) catch return error.InvalidFormat;

    if (month == 0 or month > 12) return error.InvalidDate;
    const epoch = std.time.epoch;
    const month_enum: epoch.Month = @enumFromInt(month);
    const max_day = epoch.getDaysInMonth(year, month_enum);
    if (day == 0 or day > max_day) return error.InvalidDate;

    if (time_part.len < 8) return error.InvalidFormat;
    if (time_part[2] != ':' or time_part[5] != ':') return error.InvalidFormat;

    const hour = std.fmt.parseInt(u8, time_part[0..2], 10) catch return error.InvalidFormat;
    const minute = std.fmt.parseInt(u8, time_part[3..5], 10) catch return error.InvalidFormat;
    const second = std.fmt.parseInt(u8, time_part[6..8], 10) catch return error.InvalidFormat;

    if (hour > 23 or minute > 59 or second > 60) return error.InvalidDate;

    var remainder = time_part[8..];

    if (remainder.len == 0) return error.InvalidFormat;

    if (remainder[0] == '.' or remainder[0] == ',') {
        var idx: usize = 1;
        while (idx < remainder.len and std.ascii.isDigit(remainder[idx])) : (idx += 1) {}
        remainder = remainder[idx..];
        if (remainder.len == 0) return error.InvalidFormat;
    }

    var offset_seconds: i64 = 0;
    switch (remainder[0]) {
        'Z', 'z' => {
            if (remainder.len != 1) return error.InvalidFormat;
        },
        '+', '-' => {
            const sign: i64 = if (remainder[0] == '+') 1 else -1;
            remainder = remainder[1..];
            if (remainder.len < 2) return error.InvalidFormat;
            const off_hour = std.fmt.parseInt(u8, remainder[0..2], 10) catch return error.InvalidTimeZone;
            const with_colon = remainder.len >= 3 and remainder[2] == ':';
            var off_minute: u8 = 0;
            if (with_colon) {
                if (remainder.len < 5) return error.InvalidFormat;
                off_minute = std.fmt.parseInt(u8, remainder[3..5], 10) catch return error.InvalidTimeZone;
                remainder = remainder[5..];
            } else {
                if (remainder.len < 4) return error.InvalidFormat;
                off_minute = std.fmt.parseInt(u8, remainder[2..4], 10) catch return error.InvalidTimeZone;
                remainder = remainder[4..];
            }
            if (remainder.len != 0) return error.InvalidFormat;
            if (off_hour > 23 or off_minute > 59) return error.InvalidTimeZone;
            offset_seconds = sign * (@as(i64, off_hour) * 3600 + @as(i64, off_minute) * 60);
        },
        else => return error.InvalidFormat,
    }

    const day_count = daysFromCivil(@as(i32, @intCast(year)), month, day);
    const seconds = @as(i64, day_count) * seconds_per_day +
        @as(i64, hour) * 3600 +
        @as(i64, minute) * 60 +
        @as(i64, second);

    return seconds - offset_seconds;
}

fn utcSecondsToOffsetIsoDate(utc_seconds: i64, offset_minutes: i32) TimestampError![10]u8 {
    const offset_seconds = @as(i64, offset_minutes) * 60;
    const shifted = utc_seconds + offset_seconds;
    if (shifted < 0) return error.OutOfRange;
    const unsigned = std.math.cast(u64, shifted) orelse return error.OutOfRange;

    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unsigned };
    const epoch_day = epoch_seconds.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    const year = year_day.year;
    if (year < 0 or year > 9999) return error.OutOfRange;

    var buffer: [10]u8 = undefined;
    writeFourDigits(@intCast(year), buffer[0..4]);
    buffer[4] = '-';
    writeTwoDigits(@intCast(month_day.month.numeric()), buffer[5..7]);
    buffer[7] = '-';
    writeTwoDigits(@intCast(month_day.day_index + 1), buffer[8..10]);
    return buffer;
}

fn daysFromCivil(year: i32, month: u8, day: u8) i64 {
    const m: i32 = month;
    const d: i32 = day;
    var y = year;
    var mm = m;
    if (mm <= 2) {
        y -= 1;
        mm += 12;
    }

    const era = if (y >= 0) @divTrunc(y, 400) else -@divTrunc(-y, 400) - 1;
    const yoe = y - era * 400; // [0, 399]
    const doy = @divTrunc(153 * (mm - 3) + 2, 5) + d - 1; // [0, 365]
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + @divTrunc(yoe, 400) + doy;
    return @as(i64, era) * 146097 + @as(i64, doe) - 719468;
}

fn writeFourDigits(value: u16, dest: []u8) void {
    dest[0] = toDigit(@divTrunc(value, 1000) % 10);
    dest[1] = toDigit(@divTrunc(value, 100) % 10);
    dest[2] = toDigit(@divTrunc(value, 10) % 10);
    dest[3] = toDigit(value % 10);
}

fn writeTwoDigits(value: u8, dest: []u8) void {
    dest[0] = toDigit(value / 10);
    dest[1] = toDigit(value % 10);
}

fn toDigit(value: anytype) u8 {
    return @as(u8, @intCast(value)) + '0';
}

fn posixUnixSeconds() !u64 {
    const spec = try std.posix.clock_gettime(std.posix.CLOCK.REALTIME);
    const raw_secs = if (@hasField(std.posix.timespec, "tv_sec"))
        @field(spec, "tv_sec")
    else
        @field(spec, "sec");
    return @as(u64, @intCast(raw_secs));
}

fn windowsUnixSeconds() !u64 {
    const intervals = @as(u64, @intCast(windows.ntdll.RtlGetSystemTimePrecise()));
    const WINDOWS_TO_UNIX_100NS = 11_644_473_600 * 10_000_000;
    return (intervals - WINDOWS_TO_UNIX_100NS) / 10_000_000;
}

fn detectPosixTimezoneMinutes() !i32 {
    const now = try currentUnixSeconds();
    const TimeT = c.time_t;
    const casted = std.math.cast(TimeT, @as(i64, @intCast(now))) orelse return TimezoneError.TimezoneUnavailable;
    var t_value: TimeT = casted;

    var local_tm: c.tm = undefined;
    if (c.localtime_r(&t_value, &local_tm) == null) return TimezoneError.TimezoneUnavailable;

    if (@hasField(c.tm, "tm_gmtoff")) {
        const off = @as(i64, @intCast(@field(local_tm, "tm_gmtoff")));
        return @as(i32, @intCast(@divTrunc(off, 60)));
    }
    if (@hasField(c.tm, "__tm_gmtoff")) {
        const off = @as(i64, @intCast(@field(local_tm, "__tm_gmtoff")));
        return @as(i32, @intCast(@divTrunc(off, 60)));
    }

    var utc_tm: c.tm = undefined;
    if (c.gmtime_r(&t_value, &utc_tm) == null) return TimezoneError.TimezoneUnavailable;

    const local_secs = tmToUnixSeconds(local_tm);
    const utc_secs = tmToUnixSeconds(utc_tm);
    const delta = local_secs - utc_secs;
    return @as(i32, @intCast(@divTrunc(delta, 60)));
}

fn detectWindowsTimezoneMinutes() !i32 {
    var offset_seconds: windows.LONG = 0;
    if (@hasDecl(c, "_get_timezone")) {
        if (c._get_timezone(&offset_seconds) != 0) return TimezoneError.TimezoneUnavailable;
    } else if (@hasDecl(c, "_timezone")) {
        offset_seconds = c._timezone;
    } else {
        return TimezoneError.TimezoneUnavailable;
    }
    return -@as(i32, @intCast(@divTrunc(offset_seconds, 60)));
}

fn tmToUnixSeconds(tm_value: c.tm) i64 {
    const year = @as(i32, tm_value.tm_year) + 1900;
    const month = @as(u8, @intCast(tm_value.tm_mon + 1));
    const day = @as(u8, @intCast(tm_value.tm_mday));
    const hour = tm_value.tm_hour;
    const minute = tm_value.tm_min;
    const second = tm_value.tm_sec;
    const day_count = daysFromCivil(year, month, day);
    return day_count * seconds_per_day +
        @as(i64, hour) * 3600 +
        @as(i64, minute) * 60 +
        @as(i64, second);
}

fn localtimeSafe(t_value: *c.time_t, out_tm: *c.tm) TimestampError!void {
    if (builtin.target.os.tag == .windows) {
        out_tm.* = try windowsTimeToTm(t_value, true);
        return;
    } else {
        if (c.localtime_r(t_value, out_tm) == null) return error.InvalidTimeZone;
    }
}

fn gmtimeSafe(t_value: *c.time_t, out_tm: *c.tm) TimestampError!void {
    if (builtin.target.os.tag == .windows) {
        out_tm.* = try windowsTimeToTm(t_value, false);
        return;
    } else {
        if (c.gmtime_r(t_value, out_tm) == null) return error.InvalidTimeZone;
    }
}

fn windowsTimeToTm(t_value: *c.time_t, is_local: bool) TimestampError!c.tm {
    const raw = std.math.cast(i64, t_value.*) orelse return error.OutOfRange;
    if (raw < 0) return error.OutOfRange;
    const unix_secs = @as(u64, @intCast(raw));

    const WINDOWS_TO_UNIX_100NS: u64 = 11_644_473_600 * 10_000_000;
    const filetime_intervals = unix_secs * 10_000_000 + WINDOWS_TO_UNIX_100NS;
    const filetime = win.FILETIME{
        .dwLowDateTime = @as(win.DWORD, @intCast(filetime_intervals & 0xFFFF_FFFF)),
        .dwHighDateTime = @as(win.DWORD, @intCast(filetime_intervals >> 32)),
    };

    var utc_system: win.SYSTEMTIME = undefined;
    if (win.FileTimeToSystemTime(&filetime, &utc_system) == 0) {
        return error.InvalidTimeZone;
    }

    var system = utc_system;
    if (is_local) {
        if (win.SystemTimeToTzSpecificLocalTime(null, &utc_system, &system) == 0) {
            return error.InvalidTimeZone;
        }
    }

    return systemTimeToTm(system);
}

fn systemTimeToTm(system: win.SYSTEMTIME) c.tm {
    var tm_value: c.tm = std.mem.zeroes(c.tm);
    const year = @as(i32, @intCast(system.wYear));
    const month = @as(u8, @intCast(system.wMonth));
    const day = @as(u8, @intCast(system.wDay));
    tm_value.tm_year = year - 1900;
    tm_value.tm_mon = @as(c_int, month) - 1;
    tm_value.tm_mday = @as(c_int, day);
    tm_value.tm_hour = @as(c_int, system.wHour);
    tm_value.tm_min = @as(c_int, system.wMinute);
    tm_value.tm_sec = @as(c_int, system.wSecond);
    tm_value.tm_isdst = -1;

    const day_count = daysFromCivil(year, month, day);
    const jan1 = daysFromCivil(year, 1, 1);
    tm_value.tm_yday = @as(c_int, @intCast(day_count - jan1));
    const wday = @mod(day_count + 4, 7);
    tm_value.tm_wday = @as(c_int, @intCast(wday));
    return tm_value;
}
