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

use std::num::NonZeroUsize;

use bytesize::ByteSize;

use crate::model::{ScalingMode, ShardStats};

pub(crate) struct ScalingArbiter {
    // Threshold in B/s below which we decrease the number of shards.
    scale_down_shards_threshold: ByteSize,

    // Per shard threshold in B/s above which we increase the number of shards.
    //
    // We want scaling up to be reactive, so we first inspect the short
    // term threshold.
    //
    // However, this threshold is based on a very short window of time: 5s.
    //
    // In order to avoid having back and forth scaling up and down in response to temporary
    // punctual spikes of a few MB, we also compute what would be the long term ingestion rate
    // after scaling up, and double check that it is above the long term threshold.
    scale_up_shards_short_term_threshold: ByteSize,
    scale_up_shards_long_term_threshold: ByteSize,
    // The max increase factor of the number of shards in one scale up operation
    shard_scale_up_factor: f32,
}

impl ScalingArbiter {
    pub fn with_max_shard_ingestion_throughput(
        max_shard_throughput: ByteSize,
        shard_scale_up_factor: f32,
    ) -> ScalingArbiter {
        let max_shard_throughput_bytes = max_shard_throughput.as_u64();
        ScalingArbiter {
            scale_up_shards_short_term_threshold: ByteSize::b(max_shard_throughput_bytes * 8 / 10),
            scale_up_shards_long_term_threshold: ByteSize::b(max_shard_throughput_bytes * 3 / 10),
            scale_down_shards_threshold: ByteSize::b(max_shard_throughput_bytes * 2 / 10),
            shard_scale_up_factor,
        }
    }

    /// Computes the maximum number of shards we can have without going below
    /// the long term scale up threshold
    fn long_term_scale_up_threshold_max_shards(&self, shard_stats: ShardStats) -> usize {
        let total_long_term_ingestion_rate =
            shard_stats.avg_long_term_ingestion_rate.as_u64() * shard_stats.num_open_shards as u64;
        (total_long_term_ingestion_rate / self.scale_up_shards_long_term_threshold.as_u64())
            as usize
    }

    /// Computes the next number of shards we should have according the scaling factor
    fn scale_up_factor_target_shards(&self, shard_stats: ShardStats) -> usize {
        (shard_stats.num_open_shards as f32 * self.shard_scale_up_factor).ceil() as usize
    }

    /// Scale based on the "per shard average" metric
    ///
    /// Returns `None` when there are no open shards because in that case routers are expected to
    /// make the [`quickwit_proto::control_plane::GetOrCreateOpenShardsRequest`]
    pub(crate) fn should_scale(
        &self,
        shard_stats: ShardStats,
        min_shards: NonZeroUsize,
    ) -> Option<ScalingMode> {
        // If ingest is idle, there is nothing to do. Idle shards are automatically closed by
        // ingesters (see `quickwit_ingest::ingest_v2::idle::CloseIdleShardsTask`).
        if shard_stats.num_open_shards == 0
            || shard_stats.avg_long_term_ingestion_rate == ByteSize::default()
        {
            return None;
        }
        if shard_stats.num_open_shards < min_shards.get() {
            let num_shards_to_open = min_shards.get() - shard_stats.num_open_shards;
            let scaling_mode = ScalingMode::Up(num_shards_to_open);
            return Some(scaling_mode);
        }
        // Scale up based on the short term metric value while making sure that
        // the long term value doesn't get near the scale down threshold.
        if shard_stats.avg_short_term_ingestion_rate >= self.scale_up_shards_short_term_threshold {
            let new_calculated_num_shards = usize::min(
                self.long_term_scale_up_threshold_max_shards(shard_stats),
                self.scale_up_factor_target_shards(shard_stats),
            );

            let target_num_shards = usize::max(min_shards.get(), new_calculated_num_shards);

            if target_num_shards > shard_stats.num_open_shards {
                let num_shards_to_open = target_num_shards - shard_stats.num_open_shards;
                let scaling_mode = ScalingMode::Up(num_shards_to_open);
                return Some(scaling_mode);
            }
        }
        // On the other hand, scale down only based on the long term metric value to avoid
        // being sensitive to very short drops in ingestion
        if shard_stats.avg_long_term_ingestion_rate <= self.scale_down_shards_threshold
            && shard_stats.num_open_shards > min_shards.get()
        {
            return Some(ScalingMode::Down);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytesize::ByteSize;

    use super::ScalingArbiter;
    use crate::model::{ScalingMode, ShardStats};

    #[test]
    fn test_scaling_arbiter_one_by_one() {
        // use shard throughput 10MiB to simplify calculations
        // with a factor close to 1 shards are effectively added 1 by 1
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput(ByteSize::mib(10), 1.01);
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 0,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(0),
                    avg_long_term_ingestion_rate: ByteSize::mib(0),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(5),
                    avg_long_term_ingestion_rate: ByteSize::mib(6),
                },
                NonZeroUsize::MIN
            ),
            None
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                    avg_long_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Up(1))
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                    avg_long_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Up(1))
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(3),
                    avg_long_term_ingestion_rate: ByteSize::b(15 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Down)
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(3),
                    avg_long_term_ingestion_rate: ByteSize::b(15 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(8),
                    avg_long_term_ingestion_rate: ByteSize::mib(3),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
    }

    #[test]
    fn test_scaling_arbiter_2x() {
        // use shard throughput 10MiB to simplify calculations
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput(ByteSize::mib(10), 2.);
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 0,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(0),
                    avg_long_term_ingestion_rate: ByteSize::mib(0),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(5),
                    avg_long_term_ingestion_rate: ByteSize::mib(6),
                },
                NonZeroUsize::MIN
            ),
            None
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                    avg_long_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Up(1))
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                    avg_long_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Up(2))
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(3),
                    avg_long_term_ingestion_rate: ByteSize::b(15 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Down)
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(3),
                    avg_long_term_ingestion_rate: ByteSize::b(15 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 1,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::mib(8),
                    avg_long_term_ingestion_rate: ByteSize::b(31 * bytesize::MIB / 10),
                },
                NonZeroUsize::MIN
            ),
            None,
        );
        // Scale by just 1 if 2 would bring us too close to the scale down threshold
        assert_eq!(
            scaling_arbiter.should_scale(
                ShardStats {
                    num_open_shards: 2,
                    num_closed_shards: 0,
                    avg_short_term_ingestion_rate: ByteSize::b(81 * bytesize::MIB / 10),
                    avg_long_term_ingestion_rate: ByteSize::mib(5),
                },
                NonZeroUsize::MIN
            ),
            Some(ScalingMode::Up(1)),
        );
    }

    #[test]
    fn test_scale_up_computations() {
        // use shard throughput 10MiB to simplify calculations
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput(ByteSize::mib(10), 1.5);

        let shard_stats = ShardStats {
            num_open_shards: 0,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(0),
            avg_long_term_ingestion_rate: ByteSize::mib(0),
        };
        assert_eq!(
            scaling_arbiter.long_term_scale_up_threshold_max_shards(shard_stats),
            0
        );
        assert_eq!(
            scaling_arbiter.scale_up_factor_target_shards(shard_stats),
            0
        );

        let shard_stats = ShardStats {
            num_open_shards: 1,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(5),
            avg_long_term_ingestion_rate: ByteSize::b(61 * bytesize::MIB / 10),
        };
        assert_eq!(
            scaling_arbiter.long_term_scale_up_threshold_max_shards(shard_stats),
            2
        );
        assert_eq!(
            scaling_arbiter.scale_up_factor_target_shards(shard_stats),
            2
        );

        let shard_stats = ShardStats {
            num_open_shards: 2,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(5),
            avg_long_term_ingestion_rate: ByteSize::b(11 * bytesize::MIB / 10),
        };
        assert_eq!(
            scaling_arbiter.long_term_scale_up_threshold_max_shards(shard_stats),
            0
        );
        assert_eq!(
            scaling_arbiter.scale_up_factor_target_shards(shard_stats),
            3
        );

        let shard_stats = ShardStats {
            num_open_shards: 2,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(5),
            avg_long_term_ingestion_rate: ByteSize::b(61 * bytesize::MIB / 10),
        };
        assert_eq!(
            scaling_arbiter.long_term_scale_up_threshold_max_shards(shard_stats),
            4
        );
        assert_eq!(
            scaling_arbiter.scale_up_factor_target_shards(shard_stats),
            3
        );

        let shard_stats = ShardStats {
            num_open_shards: 5,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(5),
            avg_long_term_ingestion_rate: ByteSize::b(11 * bytesize::MIB / 10),
        };
        assert_eq!(
            scaling_arbiter.long_term_scale_up_threshold_max_shards(shard_stats),
            1
        );
        assert_eq!(
            scaling_arbiter.scale_up_factor_target_shards(shard_stats),
            8
        );
    }

    #[test]
    fn test_scaling_arbiter_idle() {
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput(ByteSize::mib(10), 1.5);

        let shard_stats = ShardStats {
            num_open_shards: 0,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(0),
            avg_long_term_ingestion_rate: ByteSize::mib(0),
        };
        let min_shards = NonZeroUsize::MIN;
        let scaling_mode = scaling_arbiter.should_scale(shard_stats, min_shards);
        assert!(scaling_mode.is_none());

        let shard_stats = ShardStats {
            num_open_shards: 1,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(0),
            avg_long_term_ingestion_rate: ByteSize::mib(0),
        };
        let min_shards = NonZeroUsize::new(2).unwrap();
        let scaling_mode = scaling_arbiter.should_scale(shard_stats, min_shards);
        assert!(scaling_mode.is_none());
    }

    #[test]
    fn test_scaling_arbiter_min_shards() {
        let scaling_arbiter =
            ScalingArbiter::with_max_shard_ingestion_throughput(ByteSize::mib(10), 1.5);

        let shard_stats = ShardStats {
            num_open_shards: 1,
            num_closed_shards: 0,
            avg_short_term_ingestion_rate: ByteSize::mib(5),
            avg_long_term_ingestion_rate: ByteSize::mib(1),
        };
        let min_shards = NonZeroUsize::new(5).unwrap();
        let scaling_mode = scaling_arbiter
            .should_scale(shard_stats, min_shards)
            .unwrap();
        assert_eq!(scaling_mode, ScalingMode::Up(4));
    }
}
