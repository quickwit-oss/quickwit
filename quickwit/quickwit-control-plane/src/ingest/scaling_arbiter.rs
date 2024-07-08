// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::model::{ScalingMode, ShardStats};

pub(crate) struct ScalingArbiter {
    // Threshold in MiB/s below which we decrease the number of shards.
    scale_down_shards_threshold_mib_per_sec: f32,
    // Threshold in MiB/s above which we increase the number of shards.
    scale_up_shards_short_term_threshold_mib_per_sec: f32,
    scale_up_shards_long_term_threshold_mib_per_sec: f32,
}

impl ScalingArbiter {
    pub fn with_max_shard_ingestion_throughput_mib_per_sec(
        max_shard_throughput_mib_per_sec: f32,
    ) -> ScalingArbiter {
        ScalingArbiter {
            scale_up_shards_short_term_threshold_mib_per_sec: max_shard_throughput_mib_per_sec
                * 0.8f32,
            scale_up_shards_long_term_threshold_mib_per_sec: max_shard_throughput_mib_per_sec
                * 0.3f32,
            scale_down_shards_threshold_mib_per_sec: max_shard_throughput_mib_per_sec * 0.2f32,
        }
    }

    pub(crate) fn should_scale(&self, shard_stats: ShardStats) -> Option<ScalingMode> {
        // We scale up based on the short term threshold to scale up more aggressively.
        if shard_stats.avg_short_term_ingestion_rate
            >= self.scale_up_shards_short_term_threshold_mib_per_sec
        {
            let long_term_ingestion_rate_after_scale_up = shard_stats.avg_long_term_ingestion_rate
                * (shard_stats.num_open_shards as f32)
                / (shard_stats.num_open_shards as f32 + 1.0f32);
            if long_term_ingestion_rate_after_scale_up
                >= self.scale_up_shards_long_term_threshold_mib_per_sec
            {
                return Some(ScalingMode::Up);
            }
        }

        // On the other hand, we scale down based on the long term ingestion rate, to avoid
        // scaling down just due to a very short drop in ingestion
        if shard_stats.avg_long_term_ingestion_rate <= self.scale_down_shards_threshold_mib_per_sec
            && shard_stats.num_open_shards > 1
        {
            return Some(ScalingMode::Down);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::ScalingArbiter;
    use crate::model::{ScalingMode, ShardStats};

    #[test]
    fn test_scaling_arbiter() {
        let scaling_arbiter = ScalingArbiter::with_max_shard_ingestion_throughput_mib_per_sec(10.0);
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 1,
                avg_short_term_ingestion_rate: 5.0,
                avg_long_term_ingestion_rate: 6.0,
            }),
            None
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 1,
                avg_short_term_ingestion_rate: 8.1,
                avg_long_term_ingestion_rate: 8.1,
            }),
            Some(ScalingMode::Up)
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 2,
                avg_short_term_ingestion_rate: 3.0,
                avg_long_term_ingestion_rate: 1.5,
            }),
            Some(ScalingMode::Down)
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 1,
                avg_short_term_ingestion_rate: 3.0,
                avg_long_term_ingestion_rate: 1.5,
            }),
            None,
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 1,
                avg_short_term_ingestion_rate: 8.0f32,
                avg_long_term_ingestion_rate: 3.0f32,
            }),
            None,
        );
    }
}
