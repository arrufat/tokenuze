const std = @import("std");

const project_info = @import("build.zig.zon");
const tokenuze_version = std.SemanticVersion.parse(project_info.version) catch unreachable;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{
        .default_target = .{
            .abi = .musl,
        },
    });
    const optimize = b.standardOptimizeOption(.{});
    const resolved_version = resolveVersion(b);

    const build_options = b.addOptions();
    build_options.addOption([]const u8, "version", b.fmt("{f}", .{resolved_version}));

    const build_options_module = build_options.createModule();

    const mod = b.addModule("tokenuze", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "build_options", .module = build_options_module },
        },
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
    exe.root_module.addImport("build_options", build_options_module);
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
    unit_tests.root_module.addImport("build_options", build_options_module);

    const cli_test_module = b.createModule(.{
        .root_source_file = b.path("src/cli.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tokenuze", .module = mod },
        },
    });
    const cli_tests = b.addTest(.{ .root_module = cli_test_module });
    cli_tests.root_module.link_libc = true;
    cli_tests.root_module.addImport("build_options", build_options_module);

    const test_step = b.step("test", "Run unit tests");
    const test_cmd = b.addRunArtifact(unit_tests);
    const cli_test_cmd = b.addRunArtifact(cli_tests);
    test_step.dependOn(&test_cmd.step);
    test_step.dependOn(&cli_test_cmd.step);
}

fn resolveVersion(b: *std.Build) std.SemanticVersion {
    if (b.option([]const u8, "version-string", "Override the version of this build")) |override| {
        return std.SemanticVersion.parse(override) catch |err| {
            std.debug.panic("Expected -Dversion-string={s} to be a semantic version: {}", .{ override, err });
        };
    }

    if (tokenuze_version.pre == null and tokenuze_version.build == null) return tokenuze_version;

    const repo_dir = b.pathFromRoot(".");

    var code: u8 = undefined;

    _ = b.runAllowFail(
        &.{ "git", "-C", repo_dir, "describe", "--tags", "--exact-match" },
        &code,
        .Ignore,
    ) catch {
        const git_hash_raw = b.runAllowFail(
            &.{ "git", "-C", repo_dir, "rev-parse", "--short", "HEAD" },
            &code,
            .Ignore,
        ) catch return tokenuze_version;
        const commit_hash = std.mem.trim(u8, git_hash_raw, " \n\r");

        const commit_count = blk: {
            const base_tag_raw = b.runAllowFail(
                &.{ "git", "-C", repo_dir, "describe", "--tags", "--match=*.0", "--abbrev=0" },
                &code,
                .Ignore,
            ) catch {
                const git_count_raw = b.runAllowFail(
                    &.{ "git", "-C", repo_dir, "rev-list", "--count", "HEAD" },
                    &code,
                    .Ignore,
                ) catch return tokenuze_version;
                break :blk std.mem.trim(u8, git_count_raw, " \n\r");
            };
            const base_tag = std.mem.trim(u8, base_tag_raw, " \n\r");

            const count_cmd = b.fmt("{s}..HEAD", .{base_tag});
            const git_count_raw = b.runAllowFail(
                &.{ "git", "-C", repo_dir, "rev-list", "--count", count_cmd },
                &code,
                .Ignore,
            ) catch return tokenuze_version;
            break :blk std.mem.trim(u8, git_count_raw, " \n\r");
        };

        return .{
            .major = tokenuze_version.major,
            .minor = tokenuze_version.minor,
            .patch = tokenuze_version.patch,
            .pre = b.fmt("dev.{s}", .{commit_count}),
            .build = commit_hash,
        };
    };

    return tokenuze_version;
}
