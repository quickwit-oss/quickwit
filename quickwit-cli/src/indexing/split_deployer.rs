/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use tokio::{sync::mpsc::Sender, task};

use crate::indexing::split::Split;
use crate::indexing::statistics::StatisticEvent;

pub async fn deploy_splits(
    splits: Vec<Split>,
    statistic_sender: Sender<StatisticEvent>,
) -> anyhow::Result<Vec<Split>> {
    let mut deploy_splits_futures = vec![];
    for split in splits {
        let movable_statistic_sender = statistic_sender.clone();
        deploy_splits_futures.push(task::spawn(async move {
            split.stage(movable_statistic_sender.clone()).await?;
            split.upload(movable_statistic_sender.clone()).await?;
            split.publish(movable_statistic_sender.clone()).await?;
            anyhow::Result::<Split>::Ok(split)
        }))
    }
    let deployed_splits_results = futures::future::try_join_all(deploy_splits_futures).await?;
    let splits: Vec<Split> = deployed_splits_results
        .into_iter()
        .filter(|item| item.is_ok())
        .map(|item| item.unwrap())
        .collect();

    Ok(splits)
}
