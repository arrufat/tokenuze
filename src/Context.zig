const std = @import("std");

const Context = @This();

allocator: std.mem.Allocator,
temp_allocator: std.mem.Allocator,
io: std.Io,
environ_map: *const std.process.Environ.Map,

pub fn withTemp(self: Context, temp_allocator: std.mem.Allocator) Context {
    var copy = self;
    copy.temp_allocator = temp_allocator;
    return copy;
}
