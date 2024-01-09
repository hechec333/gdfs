# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [gdfs.proto](#gdfs-proto)
    - [AppendChunkArg](#proto-AppendChunkArg)
    - [AppendChunkReply](#proto-AppendChunkReply)
    - [ForwardDataArg](#proto-ForwardDataArg)
    - [ForwardDataReply](#proto-ForwardDataReply)
    - [ForwardOption](#proto-ForwardOption)
    - [ReadChunkArg](#proto-ReadChunkArg)
    - [ReadChunkReply](#proto-ReadChunkReply)
    - [WriteChunkArg](#proto-WriteChunkArg)
    - [WriteChunkReply](#proto-WriteChunkReply)
  
    - [ChunkServer](#proto-ChunkServer)
  
- [Scalar Value Types](#scalar-value-types)



<a name="gdfs-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## gdfs.proto



<a name="proto-AppendChunkArg"></a>

### AppendChunkArg



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_id | [int64](#int64) |  |  |
| secondaries | [string](#string) | repeated |  |






<a name="proto-AppendChunkReply"></a>

### AppendChunkReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [int64](#int64) |  |  |






<a name="proto-ForwardDataArg"></a>

### ForwardDataArg



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_id | [int64](#int64) |  |  |
| data | [bytes](#bytes) |  |  |
| chain_order | [string](#string) | repeated |  |
| option | [ForwardOption](#proto-ForwardOption) |  |  |






<a name="proto-ForwardDataReply"></a>

### ForwardDataReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repsonce_node | [int32](#int32) |  |  |






<a name="proto-ForwardOption"></a>

### ForwardOption



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sync | [bool](#bool) |  |  |
| at_lease_responce | [int32](#int32) |  |  |
| wait | [int32](#int32) |  |  |






<a name="proto-ReadChunkArg"></a>

### ReadChunkArg



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| handle | [int64](#int64) |  |  |
| offset | [int32](#int32) |  |  |
| length | [int32](#int32) |  |  |






<a name="proto-ReadChunkReply"></a>

### ReadChunkReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |
| length | [int32](#int32) |  |  |






<a name="proto-WriteChunkArg"></a>

### WriteChunkArg



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_id | [int64](#int64) |  |  |
| offset | [int64](#int64) |  |  |
| secondaries | [string](#string) | repeated |  |






<a name="proto-WriteChunkReply"></a>

### WriteChunkReply






 

 

 


<a name="proto-ChunkServer"></a>

### ChunkServer


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GRPCReadChunk | [ReadChunkArg](#proto-ReadChunkArg) | [ReadChunkReply](#proto-ReadChunkReply) |  |
| GRPCWriteChunk | [WriteChunkArg](#proto-WriteChunkArg) | [WriteChunkReply](#proto-WriteChunkReply) |  |
| GRPCAppendChunk | [AppendChunkArg](#proto-AppendChunkArg) | [AppendChunkReply](#proto-AppendChunkReply) |  |
| GRPCForWardData | [ForwardDataArg](#proto-ForwardDataArg) | [ForwardDataReply](#proto-ForwardDataReply) |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

