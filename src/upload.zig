const std = @import("std");
const machine_id = @import("machine_id.zig");
const io_util = @import("io_util.zig");
const timeutil = @import("time.zig");
const identity = @import("identity.zig");

pub const ProviderUpload = struct {
    name: []const u8,
    daily_summary: []const u8,
    sessions_summary: []const u8,
    weekly_summary: []const u8,
};

const DEFAULT_API_URL = "http://localhost:8000";
const empty_sessions_json = "{\"sessions\":[],\"totals\":{}}";
const empty_weekly_json = "{\"weekly\":[]}";

const UploadError = error{
    Unauthorized,
    ValidationFailed,
    ServerError,
    UnexpectedResponse,
};

pub fn run(
    allocator: std.mem.Allocator,
    providers: []const ProviderUpload,
    timezone_offset_minutes: i32,
) !void {
    var env = try EnvConfig.load(allocator);
    defer env.deinit(allocator);

    if (env.api_key.len == 0) reportMissingApiKey();

    const endpoint = try buildEndpoint(allocator, env.api_url);
    defer allocator.free(endpoint);

    const machine = try machine_id.getMachineId(allocator);
    const machine_slice = machine[0..];
    std.log.info("Machine ID: {s}", .{machine_slice});

    if (providers.len == 0) return error.NoProvidersSelected;

    var tz_label_buf: [16]u8 = undefined;
    const timezone_label = timeutil.formatTimezoneLabel(&tz_label_buf, timezone_offset_minutes);

    const payload = try buildUploadPayload(allocator, machine_slice, providers, timezone_label);
    defer allocator.free(payload);

    std.log.info("Uploading summary to {s}...", .{endpoint});
    var response = sendPayload(allocator, endpoint, env.api_key, payload) catch |err| {
        std.log.err("Connection failed. Is the server running at {s}? ({s})", .{ env.api_url, @errorName(err) });
        return err;
    };
    defer response.deinit(allocator);

    try handleResponse(response);
}

const EnvConfig = struct {
    api_url: []const u8,
    api_key: []const u8,

    fn load(allocator: std.mem.Allocator) !EnvConfig {
        const raw_url = std.process.getEnvVarOwned(allocator, "DASHBOARD_API_URL") catch |err| switch (err) {
            error.EnvironmentVariableNotFound => try allocator.dupe(u8, DEFAULT_API_URL),
            else => return err,
        };
        const trimmed_url = std.mem.trim(u8, raw_url, " \n\r\t");
        const api_url = try allocator.dupe(u8, trimmed_url);
        allocator.free(raw_url);

        const raw_key = std.process.getEnvVarOwned(allocator, "DASHBOARD_API_KEY") catch |err| switch (err) {
            error.EnvironmentVariableNotFound => try allocator.dupe(u8, ""),
            else => return err,
        };
        const trimmed_key = std.mem.trim(u8, raw_key, " \n\r\t");
        const api_key = try allocator.dupe(u8, trimmed_key);
        allocator.free(raw_key);

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

const HttpResponse = struct {
    status: std.http.Status,
    body: []u8,

    fn deinit(self: *HttpResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

fn sendPayload(
    allocator: std.mem.Allocator,
    endpoint: []const u8,
    api_key: []const u8,
    payload: []u8,
) !HttpResponse {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();

    var client = std.http.Client{
        .allocator = allocator,
        .io = io_single.io(),
    };
    defer client.deinit();

    var extra_headers = [_]std.http.Header{
        .{ .name = "X-API-Key", .value = api_key },
    };

    const uri = try std.Uri.parse(endpoint);
    const protocol = std.http.Client.Protocol.fromUri(uri) orelse return error.UnsupportedUriScheme;

    var host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    const host_name = try uri.getHost(&host_buffer);

    return sendWithFallback(
        allocator,
        &client,
        uri,
        protocol,
        host_name,
        &extra_headers,
        payload,
    );
}

fn sendWithFallback(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    uri: std.Uri,
    protocol: std.http.Client.Protocol,
    host_name: std.Io.net.HostName,
    extra_headers: []const std.http.Header,
    payload: []u8,
) !HttpResponse {
    return sendOnce(client, uri, extra_headers, payload, null) catch |err| switch (err) {
        error.InvalidDnsCnameRecord => {
            std.log.warn("Zig DNS resolver failed for {s}; falling back to libc resolver", .{host_name.bytes});
            const connection = try connectWithLibcResolver(allocator, client, uri, protocol, host_name) orelse return err;
            return sendOnce(client, uri, extra_headers, payload, connection);
        },
        else => return err,
    };
}

fn sendOnce(
    client: *std.http.Client,
    uri: std.Uri,
    extra_headers: []const std.http.Header,
    payload: []u8,
    connection: ?*std.http.Client.Connection,
) !HttpResponse {
    var request = try client.request(.POST, uri, .{
        .headers = .{ .content_type = .{ .override = "application/json" } },
        .extra_headers = extra_headers,
        .connection = connection,
        .keep_alive = false,
    });
    defer request.deinit();

    try request.sendBodyComplete(payload);
    var response = try request.receiveHead(&.{});

    var transfer_buffer: [4096]u8 = undefined;
    const reader = response.reader(&transfer_buffer);
    const body = try reader.allocRemaining(client.allocator, std.Io.Limit.limited(1024 * 1024));

    return .{
        .status = response.head.status,
        .body = body,
    };
}

fn connectWithLibcResolver(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    uri: std.Uri,
    protocol: std.http.Client.Protocol,
    host_name: std.Io.net.HostName,
) !?*std.http.Client.Connection {
    const port = uri.port orelse defaultPort(protocol);
    const ipv4 = try resolveIpv4Address(allocator, host_name.bytes, port) orelse return null;

    var literal_buffer: [16]u8 = undefined;
    const literal = try std.fmt.bufPrint(&literal_buffer, "{d}.{d}.{d}.{d}", .{
        ipv4[0],
        ipv4[1],
        ipv4[2],
        ipv4[3],
    });

    const ip_host = std.Io.net.HostName.init(literal) catch return null;
    return client.connectTcpOptions(.{
        .host = ip_host,
        .port = port,
        .protocol = protocol,
        .proxied_host = host_name,
    }) catch |err| switch (err) {
        error.InvalidDnsCnameRecord => return null,
        else => return err,
    };
}

fn resolveIpv4Address(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
) !?[4]u8 {
    if (@import("builtin").target.os.tag == .windows) {
        return null;
    }

    const host_c = try std.mem.concatWithSentinel(allocator, u8, &.{host}, 0);
    defer allocator.free(host_c);

    var port_buffer: [8:0]u8 = undefined;
    const port_c = std.fmt.bufPrintZ(&port_buffer, "{d}", .{port}) catch unreachable;

    var hints: std.posix.addrinfo = .{
        .flags = .{},
        .family = std.posix.AF.INET,
        .socktype = std.posix.SOCK.STREAM,
        .protocol = std.posix.IPPROTO.TCP,
        .addrlen = 0,
        .addr = null,
        .canonname = null,
        .next = null,
    };

    var results: ?*std.posix.addrinfo = null;
    const rc = std.posix.system.getaddrinfo(host_c.ptr, port_c.ptr, &hints, &results);
    defer if (results) |ptr| std.posix.system.freeaddrinfo(ptr);

    if (@intFromEnum(rc) != 0) return null;

    var cursor = results;
    while (cursor) |entry| : (cursor = entry.next) {
        const sockaddr = entry.addr orelse continue;
        var storage: std.posix.sockaddr.in = undefined;
        const src_bytes = @as([*]const u8, @ptrCast(sockaddr))[0..@sizeOf(std.posix.sockaddr.in)];
        @memcpy(std.mem.asBytes(&storage), src_bytes);
        return @bitCast(storage.addr);
    }

    return null;
}

fn buildEndpoint(allocator: std.mem.Allocator, base: []const u8) ![]u8 {
    const trimmed = trimTrailingSlash(base);
    return std.fmt.allocPrint(allocator, "{s}/api/usage/report", .{trimmed});
}

fn buildUploadPayload(
    allocator: std.mem.Allocator,
    machine: []const u8,
    providers: []const ProviderUpload,
    timezone_label: []const u8,
) ![]u8 {
    const timestamp = try timeutil.currentTimestampIso8601(allocator);
    defer allocator.free(timestamp);

    const hostname = try identity.getHostname(allocator);
    defer allocator.free(hostname);

    const username = try identity.getUsername(allocator);
    defer allocator.free(username);

    const display_name = try std.fmt.allocPrint(allocator, "{s}@{s}", .{ username, hostname });
    defer allocator.free(display_name);

    const payload = Payload{
        .timestamp = timestamp,
        .machineId = machine,
        .hostname = hostname,
        .displayName = display_name,
        .timezone = timezone_label,
        .providers = providers,
    };
    std.log.info("Payload timestamp (UTC): {s} | timezone: {s}", .{ timestamp, timezone_label });

    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);
    var writer_state = io_util.ArrayWriter.init(&buffer, allocator);
    var stringify = std.json.Stringify{ .writer = writer_state.writer(), .options = .{} };
    try stringify.write(payload);
    return buffer.toOwnedSlice(allocator);
}

fn defaultPort(protocol: std.http.Client.Protocol) u16 {
    return switch (protocol) {
        .plain => 80,
        .tls => 443,
    };
}

fn trimTrailingSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn handleResponse(response: HttpResponse) UploadError!void {
    switch (response.status) {
        .ok => {
            std.log.info("Usage reported successfully", .{});
        },
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
            try jw.objectField("weekly");
            const trimmed_weekly = std.mem.trim(u8, provider.weekly_summary, " \n\r\t");
            try jw.write(RawJson{ .text = trimmed_weekly });
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
