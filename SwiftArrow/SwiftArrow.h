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
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
} FFI_ArrowSchema;

typedef struct FFI_ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
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
