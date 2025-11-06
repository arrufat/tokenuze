const std = @import("std");
const builtin = @import("builtin");
const windows = std.os.windows;

const c = @cImport({
    @cInclude("time.h");
});

pub const DEFAULT_TIMEZONE_OFFSET_MINUTES: i32 = 9 * 60;

pub const TimestampError = error{
    InvalidFormat,
    InvalidDate,
    InvalidTimeZone,
    OutOfRange,
};

pub const ParseTimezoneError = error{
    InvalidFormat,
    OutOfRange,
};

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

pub fn isoDateForTimezone(timestamp: []const u8, offset_minutes: i32) TimestampError![10]u8 {
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    return utcSecondsToOffsetIsoDate(utc_seconds, offset_minutes);
}

pub fn parseTimezoneOffsetMinutes(input: []const u8) ParseTimezoneError!i32 {
    const trimmed = std.mem.trim(u8, input, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidFormat;

    var idx: usize = 0;
    if (trimmed.len >= 3 and std.ascii.eqlIgnoreCase(trimmed[0..3], "utc")) {
        idx = 3;
        while (idx < trimmed.len and std.ascii.isWhitespace(trimmed[idx])) : (idx += 1) {}
        if (idx == trimmed.len) return 0;
    }

    if (idx >= trimmed.len) return error.InvalidFormat;

    var sign: i32 = 1;
    switch (trimmed[idx]) {
        '+' => idx += 1,
        '-' => {
            sign = -1;
            idx += 1;
        },
        'Z', 'z' => {
            if (idx + 1 != trimmed.len) return error.InvalidFormat;
            return 0;
        },
        else => {},
    }

    if (idx >= trimmed.len) return error.InvalidFormat;

    var hours: i32 = 0;
    var hour_digits: usize = 0;
    while (idx < trimmed.len and std.ascii.isDigit(trimmed[idx])) : (idx += 1) {
        hours = hours * 10 + (@as(i32, trimmed[idx]) - '0');
        hour_digits += 1;
        if (hour_digits > 2) return error.InvalidFormat;
    }
    if (hour_digits == 0) return error.InvalidFormat;

    var minutes: i32 = 0;
    if (idx < trimmed.len) {
        if (trimmed[idx] == ':') {
            idx += 1;
            if (idx + 2 > trimmed.len) return error.InvalidFormat;
            minutes = try parseMinutePair(trimmed[idx .. idx + 2]);
            idx += 2;
        } else if (trimmed.len - idx == 2) {
            minutes = try parseMinutePair(trimmed[idx..]);
            idx = trimmed.len;
        } else {
            return error.InvalidFormat;
        }
    }

    if (idx != trimmed.len) return error.InvalidFormat;
    if (minutes >= 60) return error.InvalidFormat;

    const total_minutes = hours * 60 + minutes;
    const offset = sign * total_minutes;
    if (offset < -12 * 60 or offset > 14 * 60) return error.OutOfRange;
    return offset;
}

fn parseMinutePair(slice: []const u8) ParseTimezoneError!i32 {
    if (slice.len != 2) return error.InvalidFormat;
    if (!std.ascii.isDigit(slice[0]) or !std.ascii.isDigit(slice[1])) return error.InvalidFormat;
    return (@as(i32, slice[0]) - '0') * 10 + (@as(i32, slice[1]) - '0');
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
    return formatTimezoneLabel(allocator, offset);
}

pub fn detectLocalTimezoneOffsetMinutes() !i32 {
    return switch (builtin.target.os.tag) {
        .windows => detectWindowsTimezoneMinutes(),
        .wasi => 0,
        else => detectPosixTimezoneMinutes(),
    };
}

pub fn formatTimezoneLabel(allocator: std.mem.Allocator, offset_minutes: i32) ![]u8 {
    const clamped = std.math.clamp(offset_minutes, -12 * 60, 14 * 60);
    const sign: u8 = if (clamped >= 0) '+' else '-';
    const abs_minutes = @abs(clamped);
    const hours = abs_minutes / 60;
    const mins = abs_minutes % 60;
    return std.fmt.allocPrint(allocator, "UTC{c}{d:0>2}:{d:0>2}", .{ sign, hours, mins });
}

fn parseIso8601ToUtcSeconds(timestamp: []const u8) TimestampError!i64 {
    const split_index = std.mem.indexOfScalar(u8, timestamp, 'T') orelse return error.InvalidFormat;
    const date_part = timestamp[0..split_index];
    const time_part = timestamp[split_index + 1 ..];

    if (date_part.len != 10) return error.InvalidFormat;
    if (date_part[4] != '-' or date_part[7] != '-') return error.InvalidFormat;

    const year = std.fmt.parseInt(u16, date_part[0..4], 10) catch return error.InvalidFormat;
    const month = std.fmt.parseInt(u8, date_part[5..7], 10) catch return error.InvalidFormat;
    const day = std.fmt.parseInt(u8, date_part[8..10], 10) catch return error.InvalidFormat;

    if (month == 0 or month > 12) return error.InvalidDate;
    const epoch = std.time.epoch;
    const month_enum = std.meta.intToEnum(epoch.Month, month) catch return error.InvalidDate;
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
    const seconds = @as(i64, day_count) * SECONDS_PER_DAY +
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

    var buffer: [10]u8 = undefined;
    writeFourDigits(year_day.year, buffer[0..4]);
    buffer[4] = '-';
    writeTwoDigits(@as(u8, month_day.month.numeric()), buffer[5..7]);
    buffer[7] = '-';
    writeTwoDigits(@as(u8, month_day.day_index) + 1, buffer[8..10]);
    return buffer;
}

fn daysFromCivil(year: i32, month_u8: u8, day_u8: u8) i64 {
    const m = @as(i32, month_u8);
    const d = @as(i32, day_u8);
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
    return day_count * SECONDS_PER_DAY +
        @as(i64, hour) * 3600 +
        @as(i64, minute) * 60 +
        @as(i64, second);
}
