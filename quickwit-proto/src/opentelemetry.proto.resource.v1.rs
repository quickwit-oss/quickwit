/// Resource information.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    /// Set of attributes that describe the resource.
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag="1")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// dropped_attributes_count is the number of dropped attributes. If the value is 0, then
    /// no attributes were dropped.
    #[prost(uint32, tag="2")]
    pub dropped_attributes_count: u32,
}
