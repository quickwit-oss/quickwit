use once_cell::sync::Lazy;
use quickwit_common::metrics::{IntCounter, new_counter};

pub struct ControlPlaneMetrics {
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,
}

impl Default for ControlPlaneMetrics {
    fn default() -> Self {
        ControlPlaneMetrics {
            restart_total: new_counter(
                "restart_total",
                "Number of control plane restart.",
                "quickwit_control_plane",
            ),
            schedule_total: new_counter(
                "schedule_total",
                "Number of control plane `schedule` operation.",
                "quickwit_control_plane",
            ),
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> = Lazy::new(ControlPlaneMetrics::default);
