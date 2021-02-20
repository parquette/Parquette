//
//  SwiftArrow.h
//  SwiftArrow
//
//  Created by Marc Prud'hommeaux on 1/25/21.
//

#import <Foundation/Foundation.h>

//! Project version number for SwiftArrow.
FOUNDATION_EXPORT double SwiftArrowVersionNumber;

//! Project version string for SwiftArrow.
FOUNDATION_EXPORT const unsigned char SwiftArrowVersionString[];

// The Arrow C data interface from: https://arrow.apache.org/docs/format/CDataInterface.html

typedef struct FFI_ArrowSchema {
    /// Mandatory. A null-terminated, UTF8-encoded string describing the data type. If the data type is nested, child types are not encoded here but in the ArrowSchema.children structures.
    ///
    /// Consumers MAY decide not to support all data types, but they should document this limitation.
    const char* format;

    /// Optional. A null-terminated, UTF8-encoded string of the field or array name. This is mainly used to reconstruct child fields of nested types.
    ///
    /// Producers MAY decide not to provide this information, and consumers MAY decide to ignore it. If omitted, MAY be NULL or an empty string.
    const char* name;

   /// Optional. A binary string describing the type’s metadata. If the data type is nested, child types are not encoded here but in the ArrowSchema.children structures.
    ///
    /// This string is not null-terminated but follows a specific format:
///
    /// int32: number of key/value pairs (noted N below)
    /// int32: byte length of key 0
    /// key 0 (not null-terminated)
    /// int32: byte length of value 0
    /// value 0 (not null-terminated)
    /// ...
    /// int32: byte length of key N - 1
    /// key N - 1 (not null-terminated)
    /// int32: byte length of value N - 1
    /// value N - 1 (not null-terminated)
    /// Integers are stored in native endianness. For example, the metadata [('key1', 'value1')] is encoded on a little-endian machine as:
    ///
    /// \x01\x00\x00\x00\x04\x00\x00\x00key1\x06\x00\x00\x00value1
    /// On a big-endian machine, the same example would be encoded as:
    ///
    /// \x00\x00\x00\x01\x00\x00\x00\x04key1\x00\x00\x00\x06value1
    /// If omitted, this field MUST be NULL (not an empty string).
    ///
    /// Consumers MAY choose to ignore this information.
    const char* metadata;

    /// Optional. A bitfield of flags enriching the type description. Its value is computed by OR’ing together the flag values. The following flags are available:
    int64_t flags;

    /// Mandatory. The number of children this type has.
    int64_t n_children;

    /// Optional. A C array of pointers to each child type of this type. There must be ArrowSchema.n_children pointers.
    struct ArrowSchema** children;

    /// Optional. A pointer to the type of dictionary values.
    struct ArrowSchema* dictionary;

    /// Mandatory. A pointer to a producer-provided release callback.
    void (*release)(struct ArrowSchema*);

    /// Optional. An opaque pointer to producer-provided private data.
    ///
    /// Consumers MUST not process this member. Lifetime of this member is handled by the producer, and especially by the release callback.
    void* private_data;
} FFI_ArrowSchema;

typedef struct FFI_ArrowArray {
    /// Mandatory. The logical length of the array (i.e. its number of items).
    int64_t length;

    /// Mandatory. The number of null items in the array. MAY be -1 if not yet computed.
    int64_t null_count;

    /// Mandatory. The logical offset inside the array (i.e. the number of items from the physical start of the buffers). MUST be 0 or positive.
    int64_t offset;

    /// Mandatory. The number of physical buffers backing this array. The number of buffers is a function of the data type, as described in the Columnar format specification.
    int64_t n_buffers;

    /// Mandatory. The number of children this array has. The number of children is a function of the data type, as described in the Columnar format specification.
    int64_t n_children;

    /// Mandatory. A C array of pointers to the start of each physical buffer backing this array. Each void* pointer is the physical start of a contiguous buffer. There must be ArrowArray.n_buffers pointers.
    const void** buffers;

    /// Optional. A C array of pointers to each child array of this array. There must be ArrowArray.n_children pointers.
    struct ArrowArray** children;

    /// Optional. A pointer to the underlying array of dictionary values.
    struct ArrowArray* dictionary;

    /// Mandatory. A pointer to a producer-provided release callback.
    void (*release)(struct ArrowArray*);

    /// Optional. An opaque pointer to producer-provided private data.
    void* private_data;
} FFI_ArrowArray;

//typedef struct ArrowSchemaArray {
//  const FFI_ArrowArray *array;
//  const FFI_ArrowSchema *schema;
//} ArrowSchemaArray;

//typedef struct CompletedCallback {
//  void *userdata;
//  void (*callback)(void*, bool);
//} CompletedCallback;
//
//typedef struct CompletedCallback {
//    void * _Nonnull userdata;
//    void (* _Nonnull callback)(void * _Nonnull, bool);
//} CompletedCallback;

//void async_operation(CompletedCallback callback);


// #define Arc_Vec_ArrowVectorFFI struct Arc_Vec_ArrowVectorFFI

#define ArrowArray struct OpaqueArrowArray

#define ExecutionContext struct OpaqueExecutionContext

//#define Arc_Vec_ArrowVectorFFI struct OpaqueArc_Vec_ArrowVectorFFI

#import "arcolyte.h"
