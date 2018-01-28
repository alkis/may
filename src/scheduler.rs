use std::cell::{Cell, RefCell};
use std::io;
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::{Arc, Once, ONCE_INIT};

use crossbeam_deque;
use num_cpus;
use timeout_list::{now, TimeoutHandle, TimeOutList};
use sync::AtomicOption;
use pool::CoroutinePool;
use yield_now::set_co_para;
use io::{Selector, SysEvent};
use crossbeam::sync::SegQueue as mpmc;
use may_queue::mpmc_bounded::Queue as WaitList;
use coroutine_impl::{is_coroutine, run_coroutine, CoroutineImpl};
use smallvec::SmallVec;

#[cfg(nightly)]
use std::intrinsics::likely;
#[cfg(not(nightly))]
#[inline]
fn likely(e: bool) -> bool {
    e
}

// here we use Arc<AtomicOption<>> for that in the select implementation
// other event may try to consume the coroutine while timer thread consume it
type TimerData = Arc<AtomicOption<CoroutineImpl>>;

// filter out the cancel panic, don't print anything for it
fn filter_cancel_panic() {
    use std::panic;
    use generator::Error;
    let old = panic::take_hook();
    ::std::panic::set_hook(Box::new(move |info| {
        match info.payload().downcast_ref::<Error>() {
            // this is not an error at all, ignore it
            Some(_e @ &Error::Cancel) => return,
            _ => {}
        }
        old(info);
    }));
}

struct Runtime {
    schedulers: &'static [Scheduler],
    num_stealing: AtomicUsize,
    parked: WaitList<thread::Thread>,
    ready_list: mpmc<CoroutineImpl>,
}

static mut RUNTIME: *const Runtime = 0 as *const _;

#[inline(always)]
fn get_runtime() -> &'static Runtime {
    unsafe {
        assert!(!RUNTIME.is_null(), "called before init_runtime!");
        &* RUNTIME
    }
}

thread_local! {
    static CURRENT_SCHEDULER: Cell<*const Scheduler> = Cell::new(0 as *const _);
}

#[cold]
#[inline(never)]
fn init_runtime() {
    let cpus = num_cpus::get();
    let runtime = Runtime {
        schedulers: {
            let s: Vec<_> = (0..cpus).map(|id| Scheduler::new(id)).collect();
            unsafe { &* Box::into_raw(s.into_boxed_slice()) }
        },
        num_stealing: ATOMIC_USIZE_INIT,
        parked: WaitList::with_capacity(cpus),
        ready_list: mpmc::new(),
    };
    unsafe {
        RUNTIME = Box::into_raw(Box::new(runtime));
    }

    let counter = Arc::new(AtomicUsize::new(0));
    for id in 0..cpus {
        let counter2 = counter.clone();
        thread::Builder::new().name(format!("may-worker-{:03}", id)).spawn(move || {
            filter_cancel_panic();
            let runtime = unsafe { &*RUNTIME };
            CURRENT_SCHEDULER.with(|sched| {
                sched.set(&runtime.schedulers[id]);
            });
            counter2.fetch_add(1, Ordering::Release);
            runtime.schedulers[id].run();
        });
    }
    while counter.load(Ordering::Acquire) != cpus {

    }
}

fn schedule_global(co: CoroutineImpl) {
    let runtime = get_runtime();
    runtime.ready_list.push(co);
    if let Some(t) = runtime.parked.pop() {
        t.unpark();
    }
}

fn get_scheduler_no_coroutine() -> &'static Scheduler {
    assert!(!is_coroutine());
    static ONCE: Once = ONCE_INIT;
    ONCE.call_once(init_runtime);
    let runtime = unsafe { &*RUNTIME };
    // It does not matter which scheduler we return since this can only call
    // schedule_global().
    runtime.schedulers.last().unwrap()
}

struct FastRng {
    state: u64,
}

impl FastRng {
    fn new(seed: u64) -> FastRng {
        FastRng {
            state: seed,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // LCG constants from https://en.wikipedia.org/wiki/Numerical_Recipes.
        self.state = self.state.wrapping_mul(1664525).wrapping_add(1013904223);
        self.state
    }
}

#[inline]
pub fn get_scheduler() -> &'static Scheduler {
    CURRENT_SCHEDULER.with(|sched| {
        unsafe {
            if likely(!sched.get().is_null()) {
                &*sched.get()
            } else {
                get_scheduler_no_coroutine()
            }
        }
    })
}

pub struct Scheduler {
    pub pool: CoroutinePool,
    id: usize,
    selector: Selector,
    ready_list: crossbeam_deque::Deque<CoroutineImpl>,
    yield_list: RefCell<SmallVec<[CoroutineImpl; 16]>>,
    timer_list: TimeOutList<TimerData>,
    remove_list: RefCell<SmallVec<[TimeoutHandle<TimerData>; 16]>>,
}

fn handle_timer(co: Arc<AtomicOption<CoroutineImpl>>) {
    co.take_fast(Ordering::Acquire).map(|mut co| {
        set_co_para(&mut co, io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        get_scheduler().schedule(co);
    });
}

impl Scheduler {
    pub fn new(id: usize) -> Scheduler {
        Scheduler {
            pool: CoroutinePool::new(),
            id,
            selector: Selector::new().expect("can't create selector"),
            ready_list: crossbeam_deque::Deque::new(),
            yield_list: RefCell::new(SmallVec::new()),
            timer_list: TimeOutList::new(),
            remove_list: RefCell::new(SmallVec::new()),
        }
    }

    fn steal(&self) {
        let runtime = get_runtime();
        runtime.num_stealing.fetch_add(1, Ordering::Release);
        // Schedule one coroutine from global list if it exists.
        if let Some(co) = runtime.ready_list.try_pop() {
            self.ready_list.push(co);
            // If there are still global coroutines to run, increase parallelism...
            if !runtime.ready_list.is_empty() {
                // ... if possible.
                if let Some(t) = runtime.parked.pop() {
                    t.unpark();
                }
            }
            runtime.num_stealing.fetch_sub(1, Ordering::Release);
            return;
        }
        let deadline = now() + 1_000_000;  // 1ms TODO: Tune this
        let mut rng = FastRng::new(deadline);
        loop {
            let id = rng.next_u64() as usize % runtime.schedulers.len();
            if id == self.id {
                continue;
            } else {
                use crossbeam_deque::Steal;
                let victim = &runtime.schedulers[id];
                let stolen = loop {
                    match victim.ready_list.steal() {
                        Steal::Empty => break None,
                        Steal::Data(d) => break Some(d),
                        Steal::Retry => {},
                    }
                };
                if let Some(co) = stolen {
                    self.ready_list.push(co);
                    break;
                }
            }
            if now() > deadline {
                break;
            }
        }
        runtime.num_stealing.fetch_sub(1, Ordering::Release);
    }

    fn run(&self) {
        let runtime = get_runtime();
        // Park right on start. The first coroutine that comes in will unpark one.
        runtime.parked.push(thread::current());
        thread::park();

        let mut events: [SysEvent; 128] = unsafe { ::std::mem::uninitialized() };
        let mut timeout = Some(0);
        loop {
            // Remove canceled timers.
            while let Some(h) = self.remove_list.borrow_mut().pop() {
                h.remove();
            }
            // Schedule ready coroutines that yielded back into the readylist.
            while let Some(co) = self.yield_list.borrow_mut().pop() {
                self.ready_list.push(co);
            }
            // Check for I/O readyness.
            self.selector.select(&mut events, timeout).expect("selector error");
            // Schedule expired timers.
            timeout = self.timer_list.schedule_timer(now(), &handle_timer);
            // Increase paralellism if necessary...
            if self.ready_list.len() > 1 && runtime.num_stealing.load(Ordering::Acquire) == 0 {
                // ... and possible.
                if let Some(t) = runtime.parked.pop() {
                    t.unpark();
                }
            }
            // Run all coutines in the readylist.
            while let Some(co) = self.ready_list.pop() {
                run_coroutine(co);
            }
            // CPU bound coroutines (should) yield periodically, which puts them into the
            // the yieldlist. If we have some of those, schedule them, and start over.
            if !self.yield_list.borrow().is_empty() {
                timeout = Some(0);
                while let Some(co) = self.yield_list.borrow_mut().pop() {
                    self.ready_list.push(co);
                }
                continue;
            }
            // Nothing to run, lets steal!
            assert!(self.ready_list.is_empty());
            self.steal();
            // If we stole something continue running.
            if !self.ready_list.is_empty() {
                timeout = Some(0);
                continue;
            }
            // We didn't steal anything, or someone else stole what we just stole.
            runtime.parked.push(thread::current());
            thread::park();
        }
    }

    /// put the coroutine to ready list so that next time it can be scheduled
    #[inline]
    pub fn schedule(&self, co: CoroutineImpl) {
        if unsafe { likely(is_coroutine()) } {
            self.ready_list.push(co);
        } else {
            schedule_global(co);
        }
    }

    #[inline]
    pub fn add_timer(
        &self,
        dur: Duration,
        co: Arc<AtomicOption<CoroutineImpl>>,
    ) -> TimeoutHandle<TimerData> {
        assert!(is_coroutine());
        self.timer_list.add_timer(dur, co).0
    }

    #[inline]
    pub fn del_timer(&self, handle: TimeoutHandle<TimerData>) {
        assert!(is_coroutine());
        self.remove_list.borrow_mut().push(handle);
    }

    #[inline]
    pub fn get_selector(&self) -> &Selector {
        assert!(is_coroutine());
        &self.selector
    }
}
