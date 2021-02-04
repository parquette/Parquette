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

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
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
};

#define FFI_ArrowSchema struct ArrowSchema
#define XXXFFI_ArrowArrayXXX struct ArrowSchema


struct ArrowArray {
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
};


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



#define FFI_ArrowArray struct ArrowArray

#define ExecutionContext struct OpaqueExecutionContext

#import "arcolyte.h"
