use std::os::raw::c_uint;

use crate::sdk::datenlord_bytes;

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_error {
    pub code: c_uint,
    pub message: datenlord_bytes,
}

impl datenlord_error {
    pub fn new(code: c_uint, message: String) -> *mut datenlord_error {
        let message_bytes = message.into_bytes();
        let error = Box::new(datenlord_error {
            code,
            message: datenlord_bytes {
                data: message_bytes.as_ptr(),
                len: message_bytes.len(),
            },
        });
        Box::into_raw(error)
    }
}
