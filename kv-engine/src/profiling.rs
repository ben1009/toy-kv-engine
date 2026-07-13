//! Optional low-overhead profiling hooks.
//!
//! The `hotpath-profile` feature enables broad phase timing through hotpath-rs.
//! Without that feature, `profile_scope!` evaluates its body directly and adds
//! no runtime dependency or hot-path measurement cost.

#[cfg(feature = "hotpath-profile")]
pub use hotpath::HotpathGuard;

#[cfg(not(feature = "hotpath-profile"))]
pub struct HotpathGuard;

#[cfg(feature = "hotpath-profile")]
pub fn start_hotpath_profile(app_name: &'static str) -> Option<HotpathGuard> {
    Some(
        hotpath::HotpathGuardBuilder::new(app_name)
            .percentiles(&[50.0, 95.0, 99.0])
            .functions_limit(64)
            .threads_limit(8)
            .sections(vec![
                hotpath::Section::FunctionsTiming,
                hotpath::Section::Threads,
            ])
            .format(hotpath::Format::Table)
            .build(),
    )
}

#[cfg(not(feature = "hotpath-profile"))]
pub fn start_hotpath_profile(_app_name: &'static str) -> Option<HotpathGuard> {
    None
}

#[macro_export]
macro_rules! profile_scope {
    ($label:expr, $expr:expr) => {{
        #[cfg(feature = "hotpath-profile")]
        {
            hotpath::measure_block!($label, $expr)
        }
        #[cfg(not(feature = "hotpath-profile"))]
        {
            $expr
        }
    }};
}
