// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::model::{ScalingMode, ShardStats};

pub(crate) struct ScalingArbiter {
    // Threshold in MiB/s below which we decrease the number of shards.
    scale_down_shards_threshold_mib_per_sec: f32,

    // Threshold in MiB/s above which we increase the number of shards.
    //
    // We want scaling up to be reactive, so we first inspect the short
    // term threshold.
    //
    // However, this threshold is based on a very short window of time: 5s.
    //
    // In order to avoid having back and forth scaling up and down in response to temporary
    // punctual spikes of a few MB, we also compute what would be the long term ingestion rate
    // after scaling up, and double check that it is above the long term threshold.
    scale_up_shards_short_term_threshold_mib_per_sec: f32,
    scale_up_shards_long_term_threshold_mib_per_sec: f32,
    // The max increase factor of the number of shards in one scaling operation
    shard_scaling_factor: f32,
}

impl ScalingArbiter {
    pub fn with_max_shard_ingestion_throughput_mib_per_sec(
        max_shard_throughput_mib_per_sec: f32,
        shard_scaling_factor: f32,
    ) -> ScalingArbiter {
        ScalingArbiter {
            scale_up_shards_short_term_threshold_mib_per_sec: max_shard_throughput_mib_per_sec
                * 0.8f32,
            scale_up_shards_long_term_threshold_mib_per_sec: max_shard_throughput_mib_per_sec
                * 0.3f32,
            scale_down_shards_threshold_mib_per_sec: max_shard_throughput_mib_per_sec * 0.2f32,
            shard_scaling_factor,
        }
    }

    // Scale based on the "per shard average" metric
    pub(crate) fn should_scale(&self, shard_stats: ShardStats) -> Option<ScalingMode> {
        // Scale up based on the short term metric value while making sure that
        // the long term value doesn't get near the scale down threshold.
        if shard_stats.avg_short_term_ingestion_rate
            >= self.scale_up_shards_short_term_threshold_mib_per_sec
        {
            // compute the maximum number of shards we can add without going below
            // the long term scale up threshold
            let max_number_shards = (shard_stats.avg_long_term_ingestion_rate
                * shard_stats.num_open_shards as f32
                / self.scale_up_shards_long_term_threshold_mib_per_sec)
                .floor() as usize;

            // compute the next number of shards we should have according the scaling factor
            let target_number_shards =
                (shard_stats.num_open_shards as f32 * self.shard_scaling_factor).ceil() as usize;

            let new_number_shards = max_number_shards.min(target_number_shards);

            if new_number_shards > shard_stats.num_open_shards {
                return Some(ScalingMode::Up(
                    new_number_shards - shard_stats.num_open_shards,
                ));
            }
        }

        // On the other hand, scale down only based on the long term metric value to avoid
        // being sensitive to very short drops in ingestion
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
    fn test_scaling_arbiter_one_by_one() {
        // use shard throughput 10MiB to simplify calculations
        let scaling_arbiter = ScalingArbiter::with_max_shard_ingestion_throughput_mib_per_sec(
            10.0, // with a factor close to 1 shards are effectively added 1 by 1
            1.01,
        );
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
            Some(ScalingMode::Up(1))
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 2,
                avg_short_term_ingestion_rate: 8.1,
                avg_long_term_ingestion_rate: 8.1,
            }),
            Some(ScalingMode::Up(1))
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

    #[test]
    fn test_scaling_arbiter_2x() {
        // use shard throughput 10MiB to simplify calculations
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput_mib_per_sec(10.0, 2.);
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 2,
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
            Some(ScalingMode::Up(1))
        );
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 2,
                avg_short_term_ingestion_rate: 8.1,
                avg_long_term_ingestion_rate: 8.1,
            }),
            Some(ScalingMode::Up(2))
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
                avg_short_term_ingestion_rate: 8.0,
                avg_long_term_ingestion_rate: 3.1,
            }),
            None,
        );
        // Scale by just 1 if 2 would bring us too close to the scale down threshold
        assert_eq!(
            scaling_arbiter.should_scale(ShardStats {
                num_open_shards: 2,
                avg_short_term_ingestion_rate: 8.1,
                avg_long_term_ingestion_rate: 5.,
            }),
            Some(ScalingMode::Up(1)),
        );
    }
}
