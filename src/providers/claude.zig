const SessionProvider = @import("session_provider.zig");

const Provider = SessionProvider.Provider(.{
    .name = "claude",
    .sessions_dir_suffix = "/.claude/projects",
    .legacy_fallback_model = null,
    .fallback_pricing = &.{},
    .session_file_ext = ".jsonl",
    .strategy = .claude,
});

pub const collect = Provider.collect;
