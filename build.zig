const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{
        .default_target = .{
            .abi = .musl,
        },
    });
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("tokenuze", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });
    mod.link_libc = true;

    const exe = b.addExecutable(.{
        .name = "tokenuze",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "tokenuze", .module = mod },
            },
            .strip = optimize != .Debug,
        }),
    });
    exe.root_module.link_libc = true;
    if (target.result.os.tag == .linux and target.result.abi == .musl) {
        exe.linkage = .static;
    }

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const test_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{ .root_module = test_module });
    unit_tests.root_module.link_libc = true;
    const test_step = b.step("test", "Run unit tests");
    const test_cmd = b.addRunArtifact(unit_tests);
    test_step.dependOn(&test_cmd.step);
}
