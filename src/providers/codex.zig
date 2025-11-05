const SessionProvider = @import("session_provider.zig");

const fallback_pricing = [_]SessionProvider.FallbackPricingEntry{
    .{ .name = "gpt-5", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
    .{ .name = "gpt-5-codex", .pricing = .{
        .input_cost_per_m = 1.25,
        .cache_creation_cost_per_m = 1.25,
        .cached_input_cost_per_m = 0.125,
        .output_cost_per_m = 10.0,
    } },
};

const Provider = SessionProvider.Provider(.{
    .name = "codex",
    .sessions_dir_suffix = "/.codex/sessions",
    .legacy_fallback_model = "gpt-5",
    .fallback_pricing = fallback_pricing[0..],
});

pub const collect = Provider.collect;
