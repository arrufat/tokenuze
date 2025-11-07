const std = @import("std");
const builtin = @import("builtin");

pub fn getHostname(allocator: std.mem.Allocator) ![]u8 {
    if (std.process.getEnvVarOwned(allocator, "HOSTNAME")) |hostname| {
        return hostname;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    if (std.process.getEnvVarOwned(allocator, "COMPUTERNAME")) |computer_name| {
        return computer_name;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
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

pub fn getUsername(allocator: std.mem.Allocator) ![]u8 {
    if (std.process.getEnvVarOwned(allocator, "USER")) |user| {
        return user;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    if (std.process.getEnvVarOwned(allocator, "USERNAME")) |windows_user| {
        return windows_user;
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    return allocator.dupe(u8, "unknown");
}

fn hostnameBufferLen() usize {
    return comptime blk: {
        if (@TypeOf(std.posix.HOST_NAME_MAX) == void) break :blk 256;
        break :blk std.posix.HOST_NAME_MAX;
    };
}
