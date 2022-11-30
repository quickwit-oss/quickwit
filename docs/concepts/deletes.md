---
title: Deletes
sidebar_position: 4
---

Quickwit supports deletes thanks to the [delete API](../reference/rest-api.md#delete-api). It's important to note that this feature is mainly intended to comply with GDPR (General Data Protection Regulation) and should be used parsimoniously as deletes are expensive: typically a few queries per hour or day is recommended.

## Delete tasks

A delete task on a given index is executed on all splits created after the delete task creation. This can be a long-running task that could last several hours if the delete query is matching documents present in many splits.

To track the progress of the execution, each delete task is given a unique and incremental identifier called "operation stamp" or `opstamp`. All existing splits will undergo a delete operation and, after its success, each split metadata will be updated with the corresponding operation stamp.

All splits created after the creation of a delete tasks will have a `opstamp` greater or equal to the `opstamp` of the delete task (greater if other delete tasks have been created at the same moment).

Quickwit batches delete operations on a given split: for example, if a split has it delete `opstamp = n` and the last created delete task has a `opstamp = n + 10`, ten delete queries will be executed at once on the split.

## Delete API

Delete tasks are created through the [Delete REST API](../reference/rest-api.md#delete-api).

## Pitfalls

### Immature splits

Delete operations are applied only to “mature” splits, that is splits that do not undergo merges. Whether a split is mature depends on the [merge policy](../configuration/index-config.md#merge-policies). It is possible to define `maturation_period` after which a split will be mature. Thus, a delete request created at `t0` will first apply deletes to mature splits and, in the worst case, will wait the `t0 maturation_period` for immature splits to become mature.


### Monitoring and dev XP

It's currently not possible to monitor delete operations. An [issue](https://github.com/quickwit-oss/quickwit/issues/2494) is opened to improve the dev experience, don't hesitate to add your comments it and follow its progress.
