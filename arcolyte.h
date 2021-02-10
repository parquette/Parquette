#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * An Apache Arrow buffer
 */
typedef struct ArrowFile ArrowFile;

typedef struct DataFrameState DataFrameState;

typedef struct SerdePoint {
  int32_t x;
  int32_t y;
} SerdePoint;

typedef struct ArrowVectorFFI {
  const FFI_ArrowArray *array;
  const FFI_ArrowSchema *schema;
} ArrowVectorFFI;

typedef ArrowArray ExtArrowArray;

typedef struct CallbackT_bool {
  void *userdata;
  void (*callback)(void*, bool);
} CallbackT_bool;

typedef struct CallbackT_bool CallbackBool;

typedef struct CallbackT_i64 {
  void *userdata;
  void (*callback)(void*, int64_t);
} CallbackT_i64;

typedef struct CallbackT_i64 CallbackInt64;

/**
 * Initialize the global logger and log to `rest_client.log`.
 *
 * Note that this is an idempotent function, so you can call it as many
 * times as you want and logging will only be initialized the first time.
 */
void initialize_logging(void);

/**
 * Calculate the number of bytes in the last error's error message **not**
 * including any trailing `null` characters.
 */
int last_error_length(void);

/**
 * Write the most recent error message into a caller-provided buffer as a UTF-8
 * string, returning the number of bytes written.
 *
 * # Note
 *
 * This writes a **UTF-8** string into the buffer. Windows users may need to
 * convert it to a UTF-16 "unicode" afterwards.
 *
 * If there are no recent errors then this returns `0` (because we wrote 0
 * bytes). `-1` is returned if there are any errors, for example when passed a
 * null pointer or a buffer of insufficient size.
 */
int last_error_message(char *buffer, int length);

void hello_arcolyte(void);

/**
 * Add two signed integers.
 *
 * On a 64-bit system, arguments are 32 bit and return type is 64 bit.
 */
long long add_numbers(int x, int y);

/**
 * Take a zero-terminated C string and return its length as a
 * machine-size integer.
 */
unsigned long string_length(const char *sz_msg);

void test_schema_equality(void);

void arrow_ffi(void);

void json_to_arrow(void);

void arrow_to_json(void);

void arrow_ffi_test(const FFI_ArrowArray *array, const FFI_ArrowSchema *schema);

struct SerdePoint serde_demo(void);

char *rust_hello(const char *to);

void rust_hello_free(char *s);

void load_arrow_file(char *fname);

struct ArrowVectorFFI arrow_array_ffi_roundtrip(const struct ArrowVectorFFI *arrow);

void arrow_array_ffi_arg_param_demo(FFI_ArrowArray buf, int64_t param);

struct ArrowFile *arrow_load_csv(const char *fname, int64_t rowcount);

struct DataFrameState *datafusion_context_read_csv(ExecutionContext *ptr, const char *file_name);

struct DataFrameState *datafusion_context_read_parquet(ExecutionContext *ptr,
                                                       const char *file_name);

/**
 * Destroy a `DataFrame` once you are done with it.
 */
void datafusion_dataframe_destroy(struct DataFrameState *ptr);

/**
 * E.g.: `"SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100"`
 */
bool *datafusion_context_check_sql(ExecutionContext *ptr, const char *sql);

struct DataFrameState *datafusion_context_execute_sql(ExecutionContext *ptr, const char *sql);

/**
 * Applies the specified row limit to this data frame
 */
struct DataFrameState *datafusion_dataframe_limit(struct DataFrameState *ptr, uintptr_t count);

/**
 * Destroy an `ArrowArray` once you are done with it.
 */
void datafusion_arrow_destroy(ArrowArray *ptr);

ExtArrowArray *datafusion_array_empty_create(void);

struct ArrowVectorFFI *datafusion_dataframe_collect_vector(struct DataFrameState *ptr,
                                                           uintptr_t index);

const FFI_ArrowArray *datafusion_array_array_get(ArrowArray array);

const FFI_ArrowSchema *datafusion_array_schema_get(ArrowArray array);

ExecutionContext *datafusion_context_create(void);

/**
 * Destroy an `ExecutionContext` once you are done with it.
 */
void datafusion_context_destroy(ExecutionContext *ptr);

void datafusion_context_register_csv(ExecutionContext *ptr,
                                     const char *file_name,
                                     const char *table_name);

void datafusion_context_register_parquet(ExecutionContext *ptr,
                                         const char *file_name,
                                         const char *table_name);

void callback_bool_after(uint64_t millis, CallbackBool callback);

void callback_int64_after(uint64_t millis, int64_t value, CallbackInt64 callback);
