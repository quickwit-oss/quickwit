use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter, IntCounter};

pub struct ControlPlaneMetrics {
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,
    pub metastore_error_aborted: IntCounter,
    pub metastore_error_maybe_executed: IntCounter,
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
            metastore_error_aborted: new_counter(
                "metastore_error_aborted",
                "Number of aborted metastore transaction (= do not trigger a control plane restart)",
                "quickwit_control_plane",
            ),
            metastore_error_maybe_executed: new_counter(
                "metastore_error_maybe_executed",
                "Number of metastore transaction with an uncertain outcome (= do trigger a control plane restart)",
                "quickwit_control_plane",
            ),
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> =
    Lazy::new(ControlPlaneMetrics::default);
