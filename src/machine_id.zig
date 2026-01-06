const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const builtin = @import("builtin");

const identity = @import("identity.zig");

pub const MachineIdSource = enum {
    hardware_uuid,
    machine_id,
    mac_address,
    hostname_user,
};

pub fn getMachineId(
    allocator: Allocator,
    io: Io,
    environ_map: *const std.process.Environ.Map,
) ![16]u8 {
    if (try readCachedMachineId(allocator, io, environ_map)) |cached| {
        return cached;
    }

    const generated = try generateMachineId(allocator, io, environ_map);
    try persistMachineId(allocator, io, environ_map, generated);
    return generated;
}

fn generateMachineId(allocator: Allocator, io: Io, environ_map: *const std.process.Environ.Map) ![16]u8 {
    var unique = try selectUniqueIdentifier(allocator, io, environ_map);
    defer allocator.free(unique.value);

    return hashIdentifier(allocator, unique.value, unique.source);
}

const SelectedIdentifier = struct {
    value: []u8,
    source: MachineIdSource,
};

fn selectUniqueIdentifier(allocator: Allocator, io: Io, environ_map: *const std.process.Environ.Map) !SelectedIdentifier {
    if (try getHardwareUuid(allocator, io)) |uuid| {
        return .{ .value = uuid, .source = .hardware_uuid };
    }

    if (try getLinuxMachineId(allocator, io)) |linux_id| {
        return .{ .value = linux_id, .source = .machine_id };
    }

    if (try getMacAddress(allocator, io)) |mac| {
        return .{ .value = mac, .source = .mac_address };
    }

    const fallback = try getHostnameUserFallback(allocator, environ_map);
    return .{ .value = fallback, .source = .hostname_user };
}

fn readCachedMachineId(allocator: Allocator, io: Io, environ_map: *const std.process.Environ.Map) !?[16]u8 {
    const cache_path = try cacheFilePath(allocator, environ_map);
    defer allocator.free(cache_path);

    const file = Io.Dir.openFileAbsolute(io, cache_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        error.NotDir => return null,
        else => return err,
    };
    defer file.close(io);

    var temp: [64]u8 = undefined;
    const data = try readIntoBuffer(file, io, temp[0..]);
    const trimmed = std.mem.trim(u8, data, " \n\r\t");
    if (trimmed.len != 16) return null;

    var id: [16]u8 = undefined;
    @memcpy(id[0..16], trimmed[0..16]);
    return id;
}

fn persistMachineId(allocator: Allocator, io: Io, environ_map: *const std.process.Environ.Map, id: [16]u8) !void {
    const cache_dir = try cacheDir(allocator, environ_map);
    defer allocator.free(cache_dir);

    Io.Dir.createDirAbsolute(io, cache_dir, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    const cache_path = try std.fs.path.join(allocator, &.{ cache_dir, "machine_id" });
    defer allocator.free(cache_path);

    var file = try Io.Dir.createFileAbsolute(io, cache_path, .{ .truncate = true });
    defer file.close(io);

    try file.writeStreamingAll(io, id[0..]);
    try file.writeStreamingAll(io, "\n");
}

fn cacheDir(allocator: Allocator, environ_map: *const std.process.Environ.Map) ![]u8 {
    if (environ_map.get("HOME")) |home| {
        return std.fs.path.join(allocator, &.{ home, ".ccusage" });
    }
    if (builtin.os.tag == .windows) {
        if (environ_map.get("LOCALAPPDATA")) |app_data| {
            return std.fs.path.join(allocator, &.{ app_data, "ccusage" });
        }
    }
    return error.HomeNotFound;
}

fn cacheFilePath(allocator: Allocator, environ_map: *const std.process.Environ.Map) ![]u8 {
    const dir = try cacheDir(allocator, environ_map);
    defer allocator.free(dir);
    return std.fs.path.join(allocator, &.{ dir, "machine_id" });
}

fn getHardwareUuid(allocator: Allocator, io: Io) !?[]u8 {
    if (builtin.os.tag != .macos) return null;

    const result = std.process.run(allocator, io, .{
        .argv = &.{ "/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice" },
    }) catch return null;
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) return null;
        },
        else => return null,
    }

    if (std.mem.find(u8, result.stdout, "IOPlatformUUID")) |idx| {
        var cursor = idx;
        while (cursor < result.stdout.len and result.stdout[cursor] != '"') : (cursor += 1) {}
        if (cursor >= result.stdout.len) return null;
        cursor += 1;
        const start = cursor;
        while (cursor < result.stdout.len and result.stdout[cursor] != '"') : (cursor += 1) {}
        if (cursor > result.stdout.len) return null;
        const slice = result.stdout[start..cursor];
        return try allocator.dupe(u8, slice);
    }

    return null;
}

fn getLinuxMachineId(allocator: Allocator, io: Io) !?[]u8 {
    if (builtin.os.tag != .linux) return null;

    if (try readTrimmedFile(allocator, io, "/etc/machine-id")) |content| {
        return content;
    }

    return try readTrimmedFile(allocator, io, "/var/lib/dbus/machine-id");
}

fn readTrimmedFile(allocator: Allocator, io: Io, path: []const u8) !?[]u8 {
    const file = Io.Dir.openFileAbsolute(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        error.NotDir => return null,
        else => return err,
    };
    defer file.close(io);

    var temp: [512]u8 = undefined;
    const data = try readIntoBuffer(file, io, temp[0..]);
    const trimmed = std.mem.trim(u8, data, " \n\r\t");
    if (trimmed.len == 0) return null;
    return try allocator.dupe(u8, trimmed);
}

fn readIntoBuffer(file: Io.File, io: Io, buffer: []u8) ![]u8 {
    var filled: usize = 0;
    while (filled < buffer.len) {
        const amount = try file.readStreaming(io, &.{buffer[filled..]});
        if (amount == 0) break;
        filled += amount;
    }
    return buffer[0..filled];
}

fn getMacAddress(allocator: Allocator, io: Io) !?[]u8 {
    return switch (builtin.os.tag) {
        .macos => try parseMacFromCommand(allocator, io, &.{ "/sbin/ifconfig", "en0" }, "ether "),
        .linux => try parseMacFromCommand(allocator, io, &.{ "ip", "link", "show" }, "link/ether "),
        else => null,
    };
}

fn parseMacFromCommand(
    allocator: Allocator,
    io: Io,
    argv: []const []const u8,
    needle: []const u8,
) !?[]u8 {
    const result = std.process.run(allocator, io, .{
        .argv = argv,
    }) catch return null;
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) return null;
        },
        else => return null,
    }

    const output = result.stdout;
    if (std.mem.find(u8, output, needle)) |idx| {
        var start = idx + needle.len;
        while (start < output.len and std.ascii.isWhitespace(output[start])) : (start += 1) {}
        if (start >= output.len) return null;
        var end = start;
        while (end < output.len and !std.ascii.isWhitespace(output[end])) : (end += 1) {}
        if (end <= start) return null;
        const slice = output[start..end];
        const copy = try allocator.dupe(u8, slice);
        lowercaseInPlace(copy);
        return copy;
    }

    return null;
}

fn lowercaseInPlace(bytes: []u8) void {
    for (bytes) |*b| {
        b.* = std.ascii.toLower(b.*);
    }
}

fn getHostnameUserFallback(allocator: Allocator, environ_map: *const std.process.Environ.Map) ![]u8 {
    const hostname = try identity.getHostname(allocator, environ_map);
    defer allocator.free(hostname);

    const username = try identity.getUsername(allocator, environ_map);
    defer allocator.free(username);

    return std.fmt.allocPrint(allocator, "{s}:{s}", .{ hostname, username });
}

fn hashIdentifier(allocator: Allocator, unique: []const u8, source: MachineIdSource) ![16]u8 {
    const label = sourceLabel(source);
    const payload = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ unique, label });
    defer allocator.free(payload);

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});

    const hex = std.fmt.bytesToHex(digest, .lower);
    var id: [16]u8 = undefined;
    @memcpy(id[0..16], hex[0..16]);
    return id;
}

fn sourceLabel(source: MachineIdSource) []const u8 {
    return switch (source) {
        .hardware_uuid => "hardware_uuid",
        .machine_id => "machine_id",
        .mac_address => "mac_address",
        .hostname_user => "hostname_user",
    };
}

test "hashIdentifier truncates sha256 digest" {
    const allocator = std.testing.allocator;
    const id = try hashIdentifier(allocator, "foo", .hostname_user);
    try std.testing.expectEqualSlices(u8, "3822955c8408d78b", id[0..]);
}
