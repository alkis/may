//! coroutine io utilities

#[cfg(unix)]
#[path = "sys/unix/mod.rs"]
pub(crate) mod sys;

#[cfg(windows)]
#[path = "sys/windows/mod.rs"]
pub(crate) mod sys;

use std::io;
use coroutine_impl::is_coroutine;

pub(crate) use self::sys::{add_socket, cancel, net, IoData, Selector, SysEvent};

pub trait AsIoData {
    fn as_io_data(&self) -> &IoData;
}

#[derive(Debug)]
pub(crate) struct IoContext {
    b_init: bool,
    b_co: bool,
}

impl IoContext {
    pub fn new() -> Self {
        IoContext {
            b_init: false,
            b_co: true,
        }
    }

    #[inline]
    pub fn check<F>(&self, f: F) -> io::Result<bool>
    where
        F: FnOnce() -> io::Result<()>,
    {
        if !self.b_init {
            let me = unsafe { &mut *(self as *const _ as *mut Self) };
            if !is_coroutine() {
                me.b_co = false;
                f()?;
            }
            me.b_init = true;
        }
        Ok(self.b_co)
    }
}

// export the generic IO wrapper
pub mod co_io_err;
pub use self::sys::co_io::CoIo;
