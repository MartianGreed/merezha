use crate::{Error, Service, manifest::Limits};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use wasmtime::Result;
use wasmtime::{
    Engine, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder, Trap, UpdateDeadline,
    component::{ComponentExportIndex, Instance, TypedFunc},
};

pub(crate) struct Ticker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Ticker {
    pub fn start(engine: Engine) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("merezha-epoch".into())
            .spawn(move || {
                while !flag.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(2));
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct StoreState {
    limits: StoreLimits,
    denied: bool,
    deadline: Instant,
}

impl ResourceLimiter for StoreState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool> {
        match self.limits.memory_growing(current, desired, maximum) {
            Ok(true) => Ok(true),
            other => {
                self.denied = true;
                other
            }
        }
    }
    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool> {
        match self.limits.table_growing(current, desired, maximum) {
            Ok(true) => Ok(true),
            other => {
                self.denied = true;
                other
            }
        }
    }
    fn memory_grow_failed(&mut self, _: wasmtime::Error) -> Result<()> {
        self.denied = true;
        wasmtime::bail!("memory growth failed")
    }
    fn table_grow_failed(&mut self, _: wasmtime::Error) -> Result<()> {
        self.denied = true;
        wasmtime::bail!("table growth failed")
    }
    fn memories(&self) -> usize {
        1
    }
    fn tables(&self) -> usize {
        16
    }
    fn instances(&self) -> usize {
        32
    }
}

pub(crate) fn create_store(
    engine: &Engine,
    limits: &Limits,
    fuel: u64,
    deadline: Instant,
) -> Result<Store<StoreState>> {
    let state = StoreState {
        limits: StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            .table_elements(10_000)
            .trap_on_grow_failure(true)
            .build(),
        denied: false,
        deadline,
    };
    let mut store = Store::new(engine, state);
    store.limiter(|s| s);
    store.set_fuel(fuel)?;
    store.set_epoch_deadline(1);
    store.epoch_deadline_callback(|ctx| {
        if Instant::now() >= ctx.data().deadline {
            wasmtime::bail!("invocation deadline");
        }
        Ok(UpdateDeadline::Continue(1))
    });
    Ok(store)
}

type Handler<'a> = TypedFunc<
    (&'a str,),
    (std::result::Result<crate::bounded::BoundedString, crate::bounded::BoundedString>,),
>;

pub(crate) fn handle<'a>(
    store: &mut Store<StoreState>,
    instance: Instance,
    index: &ComponentExportIndex,
) -> Result<Handler<'a>> {
    instance.get_typed_func(store, index)
}

fn classify(store: &Store<StoreState>, error: wasmtime::Error) -> Error {
    if error.is::<crate::bounded::OutputTooLarge>() {
        return Error::OutputLimit;
    }
    if Instant::now() >= store.data().deadline {
        return Error::Timeout;
    }
    if store.data().denied || error.is::<wasmtime::OutOfMemory>() {
        return Error::MemoryLimit;
    }
    if error.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
        return Error::FuelExhausted;
    }
    Error::Trap
}

pub(crate) fn call(
    store: &mut Store<StoreState>,
    service: &Service,
    input: &str,
    output_limit: usize,
) -> Result<String, Error> {
    let instance = service
        .pre
        .instantiate(&mut *store)
        .map_err(|e| classify(store, e))?;
    let func = handle(store, instance, &service.function).map_err(|_| Error::Internal)?;
    let _limit = crate::bounded::LimitGuard::enter(output_limit);
    let (result,) = func
        .call(&mut *store, (input,))
        .map_err(|e| classify(store, e))?;
    let (text, guest_error) = match result {
        Ok(s) => (s, false),
        Err(s) => (s, true),
    };
    if guest_error {
        return Err(Error::GuestError);
    }
    let output = text.0;
    Ok(output)
}
