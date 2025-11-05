const std = @import("std");
const builtin = @import("builtin");

const c = @cImport({
    @cInclude("time.h");
});

pub const TimestampError = error{
    InvalidFormat,
    InvalidDate,
    InvalidTimeZone,
    OutOfRange,
};

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

pub fn localIsoDateFromTimestamp(timestamp: []const u8) TimestampError![10]u8 {
    const utc_seconds = try parseIso8601ToUtcSeconds(timestamp);
    return utcSecondsToLocalIsoDate(utc_seconds);
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

fn utcSecondsToLocalIsoDate(utc_seconds: i64) TimestampError![10]u8 {
    const TimeT = c.time_t;
    const casted = std.math.cast(TimeT, utc_seconds) orelse return error.OutOfRange;

    var t_value: TimeT = casted;
    var tm_value: c.tm = undefined;
    if (builtin.target.os.tag == .windows) {
        const local_ptr = c.localtime(&t_value) orelse return error.OutOfRange;
        tm_value = local_ptr.*;
    } else {
        if (c.localtime_r(&t_value, &tm_value) == null) return error.OutOfRange;
    }

    const year = @as(i64, tm_value.tm_year) + 1900;
    const month = tm_value.tm_mon + 1;
    const day = tm_value.tm_mday;

    if (year < 0 or year > 9999) return error.OutOfRange;
    if (month < 1 or month > 12) return error.OutOfRange;
    if (day < 1 or day > 31) return error.OutOfRange;

    var buffer: [10]u8 = undefined;
    writeFourDigits(@as(u16, @intCast(year)), buffer[0..4]);
    buffer[4] = '-';
    writeTwoDigits(@as(u8, @intCast(month)), buffer[5..7]);
    buffer[7] = '-';
    writeTwoDigits(@as(u8, @intCast(day)), buffer[8..10]);
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
