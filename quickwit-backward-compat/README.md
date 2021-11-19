# Backward compatibility test project.

This is just a project used to test backward compatibility of Quickwit.
Right now, only SplitMetadata is test.

The build.rs script autocreates json sample files something changes in the format.
A unit test then checks that all of these sample files deserialize to the right
`SplitMetadataAndFooterOffsets` object.

