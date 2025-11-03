const std = @import("std");

pub const MILLION = 1_000_000.0;
pub const PRICING_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

pub const TokenUsage = struct {
    input_tokens: u64 = 0,
    cached_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    reasoning_output_tokens: u64 = 0,
    total_tokens: u64 = 0,

    pub fn add(self: *TokenUsage, other: TokenUsage) void {
        self.input_tokens += other.input_tokens;
        self.cached_input_tokens += other.cached_input_tokens;
        self.output_tokens += other.output_tokens;
        self.reasoning_output_tokens += other.reasoning_output_tokens;
        self.total_tokens += other.total_tokens;
    }
};

pub const TokenUsageEvent = struct {
    session_id: []const u8,
    timestamp: []const u8,
    local_iso_date: [10]u8,
    model: []const u8,
    usage: TokenUsage,
    is_fallback: bool,
};

pub const ModelPricing = struct {
    input_cost_per_m: f64,
    cached_input_cost_per_m: f64,
    output_cost_per_m: f64,
};

pub const PricingMap = std.StringHashMap(ModelPricing);
