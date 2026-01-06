const std = @import("std");

const Context = @import("Context.zig");
const http_client = @import("http_client.zig");
const HttpResponse = http_client.Response;
const identity = @import("identity.zig");
const io_util = @import("io_util.zig");
const machine_id = @import("machine_id.zig");
const timeutil = @import("time.zig");

pub const ProviderUpload = struct {
    name: []const u8,
    daily_summary: []const u8,
    sessions_summary: []const u8,
};

const DEFAULT_API_URL = "http://localhost:8000";

const UploadError = error{
    Unauthorized,
    ValidationFailed,
    ServerError,
    UnexpectedResponse,
};

pub fn run(
    ctx: Context,
    providers: []const ProviderUpload,
    timezone_offset_minutes: i32,
) !void {
    var env_cfg = try EnvConfig.load(ctx);
    defer env_cfg.deinit(ctx.allocator);

    if (env_cfg.api_key.len == 0) reportMissingApiKey();

    const endpoint = try buildEndpoint(ctx.allocator, env_cfg.api_url);
    defer ctx.allocator.free(endpoint);

    const machine = try machine_id.getMachineId(ctx);
    const machine_slice = machine[0..];
    std.log.debug("Machine ID: {s}", .{machine_slice});

    if (providers.len == 0) return error.NoProvidersSelected;

    var tz_label_buf: [16]u8 = undefined;
    const timezone_label = timeutil.formatTimezoneLabel(&tz_label_buf, timezone_offset_minutes);

    const payload = try buildUploadPayload(ctx, machine_slice, providers, timezone_label);
    defer ctx.allocator.free(payload);

    std.log.info("Uploading summary to {s}...", .{endpoint});
    var upload_timer: std.time.Timer = try .start();
    var response = sendPayload(ctx.allocator, endpoint, env_cfg.api_key, payload) catch |err| {
        std.log.err("Connection failed. Is the server running at {s}? ({s})", .{ env_cfg.api_url, @errorName(err) });
        return err;
    };
    defer response.deinit();

    try handleResponse(response);
    std.log.info("Usage reported successfully in {d:.2}ms", .{timeutil.nsToMs(upload_timer.read())});
}

const EnvConfig = struct {
    api_url: []const u8,
    api_key: []const u8,

    fn load(ctx: Context) !EnvConfig {
        const raw_url = ctx.environ_map.get("DASHBOARD_API_URL") orelse DEFAULT_API_URL;
        const trimmed_url = std.mem.trim(u8, raw_url, " \n\r\t");
        const api_url = try ctx.allocator.dupe(u8, trimmed_url);

        const raw_key = ctx.environ_map.get("DASHBOARD_API_KEY") orelse "";
        const trimmed_key = std.mem.trim(u8, raw_key, " \n\r\t");
        const api_key = try ctx.allocator.dupe(u8, trimmed_key);

        return .{ .api_url = api_url, .api_key = api_key };
    }

    fn deinit(self: *EnvConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.api_url);
        allocator.free(self.api_key);
        self.* = undefined;
    }
};

fn reportMissingApiKey() noreturn {
    std.debug.print("Error: DASHBOARD_API_KEY not set\n", .{});
    std.debug.print("   Please add to ~/.zshrc or ~/.bashrc:\n", .{});
    std.debug.print("   export DASHBOARD_API_KEY=\"sk_team_your_api_key_here\"\n", .{});
    std.debug.print("   (optionally set DASHBOARD_API_URL for non-default endpoints)\n", .{});
    std.process.exit(1);
}

fn sendPayload(
    allocator: std.mem.Allocator,
    endpoint: []const u8,
    api_key: []const u8,
    payload: []u8,
) !HttpResponse {
    var extra_headers = [_]std.http.Header{
        .{ .name = "X-API-Key", .value = api_key },
    };

    return http_client.request(
        allocator,
        allocator,
        .{
            .method = .POST,
            .url = endpoint,
            .body = payload,
            .content_type = "application/json",
            .extra_headers = &extra_headers,
            .response_limit = std.Io.Limit.limited(1024 * 1024),
            .stream_handler = http_client.discardStream,
        },
    );
}

fn buildEndpoint(allocator: std.mem.Allocator, base: []const u8) ![]u8 {
    const trimmed = trimTrailingSlash(base);
    return std.fmt.allocPrint(allocator, "{s}/api/usage/report", .{trimmed});
}

fn buildUploadPayload(
    ctx: Context,
    machine: []const u8,
    providers: []const ProviderUpload,
    timezone_label: []const u8,
) ![]u8 {
    const timestamp = try timeutil.currentTimestampIso8601(ctx.allocator);
    defer ctx.allocator.free(timestamp);

    const hostname = try identity.getHostname(ctx);
    defer ctx.allocator.free(hostname);

    const username = try identity.getUsername(ctx);
    defer ctx.allocator.free(username);

    const display_name = try std.fmt.allocPrint(ctx.allocator, "{s}@{s}", .{ username, hostname });
    defer ctx.allocator.free(display_name);

    const payload = Payload{
        .timestamp = timestamp,
        .machineId = machine,
        .hostname = hostname,
        .displayName = display_name,
        .timezone = timezone_label,
        .providers = providers,
    };
    std.log.debug("Payload timestamp (UTC): {s} | timezone: {s}", .{ timestamp, timezone_label });

    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(ctx.allocator);
    var writer_state = io_util.ArrayWriter.init(&buffer, ctx.allocator);
    var stringify = std.json.Stringify{ .writer = writer_state.writer(), .options = .{} };
    try stringify.write(payload);
    return buffer.toOwnedSlice(ctx.allocator);
}

fn trimTrailingSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn handleResponse(response: HttpResponse) UploadError!void {
    switch (response.status) {
        .ok => {},
        .unauthorized => {
            std.log.err("Authentication failed: Invalid or inactive API key", .{});
            return UploadError.Unauthorized;
        },
        .unprocessable_entity => {
            std.log.err("Data validation error. See server logs for details.", .{});
            return UploadError.ValidationFailed;
        },
        .internal_server_error => {
            std.log.err("Server error. Please try again later.", .{});
            return UploadError.ServerError;
        },
        else => {
            std.log.err("Failed to report usage (HTTP {d}). Check server logs for diagnostics.", .{@intFromEnum(response.status)});
            return UploadError.UnexpectedResponse;
        },
    }
}

const Payload = struct {
    timestamp: []const u8,
    machineId: []const u8,
    hostname: []const u8,
    displayName: []const u8,
    timezone: []const u8,
    providers: []const ProviderUpload,

    pub fn jsonStringify(self: Payload, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("timestamp");
        try jw.write(self.timestamp);
        try jw.objectField("machineId");
        try jw.write(self.machineId);
        try jw.objectField("hostname");
        try jw.write(self.hostname);
        try jw.objectField("displayName");
        try jw.write(self.displayName);
        try jw.objectField("timezone");
        try jw.write(self.timezone);

        for (self.providers) |provider| {
            try jw.objectField(provider.name);
            try jw.beginObject();
            try jw.objectField("sessions");
            const trimmed_sessions = std.mem.trim(u8, provider.sessions_summary, " \n\r\t");
            try jw.write(RawJson{ .text = trimmed_sessions });
            try jw.objectField("daily");
            const trimmed_daily = std.mem.trim(u8, provider.daily_summary, " \n\r\t");
            try jw.write(RawJson{ .text = trimmed_daily });
            try jw.endObject();
        }

        try jw.endObject();
    }
};

const RawJson = struct {
    text: []const u8,

    pub fn jsonStringify(self: RawJson, jw: anytype) !void {
        try jw.beginWriteRaw();
        try jw.writer.writeAll(self.text);
        jw.endWriteRaw();
    }
};

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
    const yoe = y - era * 400;
    const doy = @divTrunc(153 * (mm - 3) + 2, 5) + d - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + @divTrunc(yoe, 400) + doy;
    return @as(i64, era) * 146097 + @as(i64, doe) - 719468;
}

fn currentUnixSeconds() !u64 {
    const os = @import("builtin").target.os.tag;
    return switch (os) {
        .windows => windowsUnixSeconds(),
        else => posixUnixSeconds(),
    };
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
    const win = std.os.windows;
    const intervals = @as(u64, @intCast(win.ntdll.RtlGetSystemTimePrecise()));
    const WINDOWS_TO_UNIX_100NS = 11_644_473_600 * 10_000_000;
    return (intervals - WINDOWS_TO_UNIX_100NS) / 10_000_000;
}
