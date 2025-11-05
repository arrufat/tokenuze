const SessionProvider = @import("session_provider.zig");

const fallback_pricing = [_]SessionProvider.FallbackPricingEntry{
    .{ .name = "gemini-1.5-pro", .pricing = .{
        .input_cost_per_m = 3.50,
        .cache_creation_cost_per_m = 3.50,
        .cached_input_cost_per_m = 3.50,
        .output_cost_per_m = 10.50,
    } },
    .{ .name = "gemini-1.5-flash", .pricing = .{
        .input_cost_per_m = 0.35,
        .cache_creation_cost_per_m = 0.35,
        .cached_input_cost_per_m = 0.35,
        .output_cost_per_m = 1.05,
    } },
};

const Provider = SessionProvider.Provider(.{
    .name = "gemini",
    .sessions_dir_suffix = "/.gemini/tmp",
    .legacy_fallback_model = null,
    .fallback_pricing = fallback_pricing[0..],
    .session_file_ext = ".json",
    .strategy = .gemini,
});

pub const collect = Provider.collect;
