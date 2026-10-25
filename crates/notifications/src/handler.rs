//! The block's job handlers and their outcome mapping (NT §2.5): a
//! handler decides `Done`, `Retry { after, code }` or `Permanent { code }`,
//! mapped onto AP's `JobFailure` classification (R1).
use crate::{
    deliver, expand, maintenance,
    notifier::Slot,
    payload::{DeliverPayload, ExpandPayload},
};
use merezha_platform::{
    BoxFuture,
    ports::{Channel, JobContext, JobFailure, JobHandler},
};
use serde_json::Value;
use std::time::Duration;

/// What a handler decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Retry {
        after: Option<Duration>,
        code: String,
    },
    Permanent(String),
}
impl Outcome {
    pub fn retry(code: &str) -> Self {
        Outcome::Retry {
            after: None,
            code: code.to_owned(),
        }
    }
    pub fn into_result(self) -> Result<(), JobFailure> {
        match self {
            Outcome::Done => Ok(()),
            Outcome::Retry { after, code } => Err(JobFailure::transient(&code, after)),
            Outcome::Permanent(code) => Err(JobFailure::permanent(&code)),
        }
    }
}

/// The std deadline of a job, from its tokio one.
fn deadline(ctx: &JobContext) -> std::time::Instant {
    ctx.deadline.into_std()
}

/// `notify.expand`.
pub struct Expand(pub Slot);
impl JobHandler for Expand {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            // A flagged payload is nulled at the job's terminal state: a
            // replay of a dead request cannot be expanded again (R3).
            if payload.is_null() {
                return Err(JobFailure::permanent("secret_purged"));
            }
            let Ok(payload) = serde_json::from_value::<ExpandPayload>(payload) else {
                return Err(JobFailure::permanent("invalid_job_payload"));
            };
            expand::run(&core, deadline(&ctx), payload)
                .await
                .into_result()
        })
    }
}

/// `notify.deliver.email` and `notify.deliver.sms`.
pub struct Deliver(pub Slot, pub Channel);
impl JobHandler for Deliver {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            if payload.is_null() {
                return Err(JobFailure::permanent("secret_purged"));
            }
            let Ok(payload) = serde_json::from_value::<DeliverPayload>(payload) else {
                return Err(JobFailure::permanent("invalid_job_payload"));
            };
            deliver::run(&core, self.1, &ctx, deadline(&ctx), payload)
                .await
                .into_result()
        })
    }
}

/// `notify.maintenance.<task>`.
pub struct Maintenance(pub Slot, pub maintenance::Task);
impl JobHandler for Maintenance {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        _payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            maintenance::run(&core, self.1, deadline(&ctx))
                .await
                .map(|_| ())
                .map_err(JobFailure::from)
        })
    }
}
