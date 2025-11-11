const std = @import("std");

pub const ResponseHandler = *const fn (?*anyopaque, []const u8) anyerror!void;

pub const RequestOptions = struct {
    method: std.http.Method = .GET,
    url: []const u8,
    body: ?[]u8 = null,
    content_type: ?[]const u8 = null,
    extra_headers: []const std.http.Header = &.{},
    response_limit: std.Io.Limit = std.Io.Limit.limited(1024 * 1024),
    force_identity_encoding: bool = false,
    stream_handler: ?ResponseHandler = null,
    stream_context: ?*anyopaque = null,
};

pub const Response = struct {
    status: std.http.Status,
    body: []u8 = &.{},
    allocator: std.mem.Allocator,
    owns_body: bool = false,

    pub fn deinit(self: *Response) void {
        if (self.owns_body) {
            self.allocator.free(self.body);
        }
        self.* = undefined;
    }
};

pub fn request(
    client_allocator: std.mem.Allocator,
    response_allocator: std.mem.Allocator,
    options: RequestOptions,
) !Response {
    var io_single = std.Io.Threaded.init_single_threaded;
    defer io_single.deinit();

    var client = std.http.Client{
        .allocator = client_allocator,
        .io = io_single.io(),
    };
    defer client.deinit();

    const uri = try std.Uri.parse(options.url);
    const protocol = std.http.Client.Protocol.fromUri(uri) orelse return error.UnsupportedUriScheme;

    var host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    const host_name = try uri.getHost(&host_buffer);

    return sendWithFallback(
        client_allocator,
        response_allocator,
        &client,
        uri,
        protocol,
        host_name,
        options,
    );
}

fn sendWithFallback(
    scratch_allocator: std.mem.Allocator,
    response_allocator: std.mem.Allocator,
    client: *std.http.Client,
    uri: std.Uri,
    protocol: std.http.Client.Protocol,
    host_name: std.Io.net.HostName,
    options: RequestOptions,
) !Response {
    return sendRequest(response_allocator, client, uri, options, null) catch |err| {
        if (err != error.InvalidDnsCnameRecord) return err;

        std.log.warn("DNS resolver failed for {s}; retrying with libc resolver", .{host_name.bytes});
        const connection = try connectWithLibcResolver(scratch_allocator, client, uri, protocol, host_name) orelse return err;
        return sendRequest(response_allocator, client, uri, options, connection);
    };
}

fn sendRequest(
    response_allocator: std.mem.Allocator,
    client: *std.http.Client,
    uri: std.Uri,
    options: RequestOptions,
    connection: ?*std.http.Client.Connection,
) !Response {
    const headers = std.http.Client.Request.Headers{
        .content_type = if (options.content_type) |ct|
            .{ .override = ct }
        else
            .default,
        .accept_encoding = if (options.force_identity_encoding)
            .{ .override = "identity" }
        else
            .default,
    };

    var http_request = try client.request(options.method, uri, .{
        .headers = headers,
        .extra_headers = options.extra_headers,
        .connection = connection,
        .keep_alive = false,
    });
    defer http_request.deinit();

    if (options.body) |payload| {
        try http_request.sendBodyComplete(payload);
    } else {
        try http_request.sendBodiless();
    }

    var response = try http_request.receiveHead(&.{});

    if (options.stream_handler) |handler| {
        var transfer_buffer: [4096]u8 = undefined;
        const reader = response.reader(&transfer_buffer);
        var remaining = options.response_limit;
        while (true) {
            const got = try reader.readSliceShort(transfer_buffer[0..]);
            if (got == 0) break;
            remaining = remaining.subtract(got) orelse return error.ResponseLimitExceeded;
            try handler(options.stream_context, transfer_buffer[0..got]);
            if (got < transfer_buffer.len) {
                // readSliceShort returns fewer bytes than requested only at EOF
                break;
            }
        }
        return .{ .status = response.head.status, .allocator = response_allocator };
    } else {
        var transfer_buffer: [4096]u8 = undefined;
        const reader = response.reader(&transfer_buffer);
        const body = try reader.allocRemaining(response_allocator, options.response_limit);
        return .{
            .status = response.head.status,
            .body = body,
            .allocator = response_allocator,
            .owns_body = true,
        };
    }
}

pub fn discardStream(_: ?*anyopaque, _: []const u8) anyerror!void {}

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
    const literal = try std.fmt.bufPrint(&literal_buffer, "{d}.{d}.{d}.{d}", .{ ipv4[0], ipv4[1], ipv4[2], ipv4[3] });

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

fn defaultPort(protocol: std.http.Client.Protocol) u16 {
    return switch (protocol) {
        .plain => 80,
        .tls => 443,
    };
}
