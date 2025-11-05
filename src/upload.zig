const std = @import("std");

const DEFAULT_API_URL = "http://localhost:8000";

pub fn run(allocator: std.mem.Allocator, payload: []const u8) !void {
    var env = try EnvConfig.load(allocator);
    defer env.deinit(allocator);

    if (env.api_key.len == 0) reportMissingApiKey();

    const endpoint = try buildEndpoint(allocator, env.api_url);
    defer allocator.free(endpoint);

    std.log.info("Uploading summary to {s}...", .{endpoint});
    var response = sendPayload(allocator, endpoint, env.api_key, @constCast(payload)) catch |err| {
        std.debug.print("Connection failed. Is the server running at {s}?\n", .{env.api_url});
        return err;
    };
    defer response.deinit(allocator);

    handleResponse(response);
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

    const uri = try std.Uri.parse(endpoint);
    var extra_headers = [_]std.http.Header{
        .{ .name = "X-API-Key", .value = api_key },
    };

    var request = try client.request(.POST, uri, .{
        .headers = .{ .content_type = .{ .override = "application/json" } },
        .extra_headers = &extra_headers,
    });
    defer request.deinit();

    try request.sendBodyComplete(payload);
    var response = try request.receiveHead(&.{});

    var transfer_buffer: [4096]u8 = undefined;
    const reader = response.reader(&transfer_buffer);
    const body = try reader.allocRemaining(allocator, std.Io.Limit.limited(1024 * 1024));

    return .{
        .status = response.head.status,
        .body = body,
    };
}

fn buildEndpoint(allocator: std.mem.Allocator, base: []const u8) ![]u8 {
    const trimmed = trimTrailingSlash(base);
    return std.fmt.allocPrint(allocator, "{s}/api/usage/report", .{trimmed});
}

fn trimTrailingSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn handleResponse(response: HttpResponse) void {
    const body_trimmed = std.mem.trim(u8, response.body, " \n\r\t");
    switch (response.status) {
        .ok => {
            std.debug.print("Usage reported successfully\n", .{});
            if (body_trimmed.len > 0) std.debug.print("   Response: {s}\n", .{body_trimmed});
        },
        .unauthorized => {
            std.debug.print("Authentication failed: Invalid or inactive API key\n", .{});
        },
        .unprocessable_entity => {
            std.debug.print("Data validation error.\n", .{});
            if (body_trimmed.len > 0) std.debug.print("   Error: {s}\n", .{body_trimmed});
        },
        .internal_server_error => {
            std.debug.print("Server error. Please try again later.\n", .{});
            if (body_trimmed.len > 0) std.debug.print("   Error: {s}\n", .{body_trimmed});
        },
        else => {
            std.debug.print(
                "Failed to report usage (HTTP {d})\n",
                .{@intFromEnum(response.status)},
            );
            if (body_trimmed.len > 0) std.debug.print("   Response: {s}\n", .{body_trimmed});
        },
    }
}
