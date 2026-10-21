//! A bounded string lift for the pinned Wasmtime canonical ABI.
//!
//! Wasmtime 49 automatically runs guest post-return before `TypedFunc::call`
//! returns, so a WasmStr cursor cannot preserve result bytes across that call.
//! This wrapper checks length then delegates the owned lift to Wasmtime.
use std::cell::Cell;
use wasmtime::{
    Result,
    component::{
        __internal::{CanonicalAbiInfo, InstanceType, InterfaceType, LiftContext},
        ComponentType, Lift,
    },
};

thread_local! { static OUTPUT_LIMIT: Cell<usize> = const { Cell::new(0) }; }
pub(crate) struct LimitGuard(usize);
impl LimitGuard {
    pub fn enter(limit: usize) -> Self {
        Self(OUTPUT_LIMIT.replace(limit))
    }
}
impl Drop for LimitGuard {
    fn drop(&mut self) {
        OUTPUT_LIMIT.set(self.0);
    }
}

#[derive(Debug)]
pub(crate) struct OutputTooLarge;
impl std::fmt::Display for OutputTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("output_limit")
    }
}
impl std::error::Error for OutputTooLarge {}
pub(crate) struct BoundedString(pub String);
fn check(len: usize) -> Result<()> {
    if len > OUTPUT_LIMIT.get() {
        return Err(OutputTooLarge.into());
    }
    Ok(())
}

// SAFETY: This wrapper uses String's exact ABI representation, size, alignment
// and type check. It is only lifted, never lowered. Its owned String is Send
// and Sync. All raw value validation and copying remain in String's Lift.
unsafe impl ComponentType for BoundedString {
    type Lower = <String as ComponentType>::Lower;
    const ABI: CanonicalAbiInfo = <String as ComponentType>::ABI;
    fn typecheck(ty: &InterfaceType, types: &InstanceType<'_>) -> Result<()> {
        <String as ComponentType>::typecheck(ty, types)
    }
}

// SAFETY: The loader permits only UTF-8 linear canonical ABI and memory32.
// We read its length with the same representation used by Wasmtime 49.0.3,
// then delegate every bounds/alignment/UTF-8 check to String's implementation.
// Returning early on oversized input cannot initialize an invalid value.
unsafe impl Lift for BoundedString {
    fn linear_lift_from_flat(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        src: &Self::Lower,
    ) -> Result<Self> {
        check(src[1].get_u32() as usize)?;
        <String as Lift>::linear_lift_from_flat(cx, ty, src).map(Self)
    }
    fn linear_lift_from_memory(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        bytes: &[u8],
    ) -> Result<Self> {
        let len = u32::from_le_bytes(
            bytes
                .get(4..8)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| wasmtime::format_err!("invalid string representation"))?,
        );
        check(len as usize)?;
        <String as Lift>::linear_lift_from_memory(cx, ty, bytes).map(Self)
    }
}
