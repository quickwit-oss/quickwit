# Backward compatibility in Quickwit.

If you are reading this, chances are you want to make a change to one of the resource
of Quickwit's meta/config.

There are basically 3 types of configuration:

Edited by the user and read back from file on startup:
- QuickwitConfig

Edited by the user then stored in the metastore:
- IndexConfig
- SourceConfig
- VersionedIndexTemplate

Assembled by Quickwit then stored in the metastore:
- IndexMetadata
- SplitMetadata
- FileBackedIndex (file backed metastore only)
- Manifest (file backed metastore only)

Quickwit currently manages the backward compatibility of all of these resources except the `QuickwitConfig`.

This document describes how to handle a change, and how to make test such a change, and spot eventual regression.

## How do I update `{IndexMetadata, SplitMetadata, FileBackedIndex, SourceConfig, IndexConfig, Manifest}`?

There are two types of upgrades:
- naturally backward compatible change
- change requiring a new version

### Naturally backward compatible change

Serde offers some attributes to make backward compatible changes to our model.
For instance, it is possible to add a new field to a struct and slap
a `serde(default)` attribute to it in order to handle older serialized version of the
struct.

If you want to avoid to generate any diff on the non-regression json files,
you can also avoid use `#[serde(skip_serializing_if)]`, although by default,
it is recommended to not use it.

It is also possible to rename a field in a backward compatible manner
by using the `#[serde(alias)]`.

For this type of change it is not required to update the serialization version.

Nevertheless, the regression tests will spot these changes. When that happens:
- modify your model with the help of the attributes above.
- modify the example for the model by editing its `TestableForRegression` trait implementation.
- run the backward compatibility tests (see below)
- check the diff between the `xxx.modified.json` files created and the matching `xxx.json` files. 
If the changes are acceptable, replace the content of the `xxx.json` files and commit them.

Be particularly careful to changes on files corresponding to the most recent version. If the 
changes are not compatible, create a new configuration version.

### Change requiring a new version

For changes requiring a new version, you will have to increment the configuration
version. You need to make sure that all of these resources share the same version number.

- update the resource struct you want to change.
- create a new item in the `VersionedXXXX` struct. It is usually located in a serialize.rs file
- `Serialize` is not needed for the previous serialized version. We just need `Deserialize`. We can 
remove the `Serialize` impl from the derive statement, and mark it a `skip_serializing` as follows.

e.g.
```
#[serde(tag = "version")]
pub(crate) enum VersionedXXXXXX {
    #[serde(rename = "0")]
    V0(#[serde(skip_serializing)] XXXX_V0),
    #[serde(rename = "1")]
    V1(XXXX_V1),
}
```
- complete the conversion `From<VersionedXXXX> for XXXX` and `From<XXXX> for VersionedXXXX`
- run the backward compatibility tests (see below)
- for older versions, check the diff between the `xxx.expected.modified.json` files created and the matching `xxx.expected.json` files. 
If the changes are acceptable, replace the content of the `xxx.expected.json` files and commit them.
- check the `yyyy.json` that was created for the new version and commit it along with the `yyyy.expected.json` file (identical).
- possibly update the generation of the default XXXX instance used for regression. It is in the function `TestableForRegression::sample_for_regression`.


## Backward compatibility tests

These tests are used to ensure the backward compatibility of Quickwit.
Right now, `SplitMetadata`, `IndexMetadata`, `Manifest` and `FileBackedIndex` are tested.

We want to be able to read all past versions of these files, but only write the most recent format.

The tests consist of pairs of JSON files, `XXXX.json` and `XXXX.expected.json`:
- `XXXX.json` is the first serialized value of a new version.
- `XXXX.expected.json` is the result of `serialize_new_version(deserialize(XXXX.json))`.

Format changes are automatically detected. There are two possible situations when a format changes.

#### Updating expected.json

We need to keep `*.expected.json` files up-to-date with the format changes.

This is done in a semi-automatic fashion.

Checks are performed in two steps:
- first pass, `deserialize(original_json) == deserialize(expectation_json)`
- second pass, `expectation_json = serialize(deserialize(expectation_json))`

When changing the json format, it is expected to see this test fail.
The unit test then updates automatically the `expected.json`. The developer just has to
check the diff of the result (in particular no information should be lost) and commit the 
updated expected.json files.

Adding this update operation within the unit test is a tad unexpected, but it has the merit of
integrating well with CI. If a developer forgets to update the expected.json file,
the CI will catch it.

#### Adding a new test case.

If the serialization format changes, a new version should be created and the unit test will
automatically add a new unit test generated from the sample tested objects.
Concretely, it will just write two files `XXXX.json` and `XXXX.expected.json` for each model.

The two files will be identical. This is expected as this is a unit test for the most recent 
version. The unit test will start making sense in future updates thanks to the update phase
described in the previous section.
