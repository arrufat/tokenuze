const std = @import("std");
const builtin = @import("builtin");

pub fn getHostname(
    allocator: std.mem.Allocator,
    environ_map: *const std.process.Environ.Map,
) ![]u8 {
    const host_vars = [_][]const u8{ "HOSTNAME", "COMPUTERNAME" };
    for (host_vars) |var_name| {
        if (environ_map.get(var_name)) |hostname| {
            return allocator.dupe(u8, hostname);
        }
    }

    if (builtin.target.os.tag == .windows) {
        return allocator.dupe(u8, "unknown-host");
    } else {
        var buf: [hostnameBufferLen()]u8 = undefined;
        const name = std.posix.gethostname(&buf) catch {
            return allocator.dupe(u8, "unknown-host");
        };
        return allocator.dupe(u8, name);
    }
}

pub fn getUsername(
    allocator: std.mem.Allocator,
    environ_map: *const std.process.Environ.Map,
) ![]u8 {
    const user_vars = [_][]const u8{ "USER", "USERNAME" };
    for (user_vars) |var_name| {
        if (environ_map.get(var_name)) |username| {
            return allocator.dupe(u8, username);
        }
    }

    return allocator.dupe(u8, "unknown");
}

fn hostnameBufferLen() usize {
    return comptime blk: {
        if (@TypeOf(std.posix.HOST_NAME_MAX) == void) break :blk 256;
        break :blk std.posix.HOST_NAME_MAX;
    };
}
