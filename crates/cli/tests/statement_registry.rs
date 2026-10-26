//! The registry the `merezha` binary runs with (TE §1.3 "PostgreSQL"):
//! the kernel's statements and every block's, which must not share a name,
//! since `merezha.db.statement` tells them apart on spans and series.
use merezha_postgres::{KERNEL_STATEMENTS, check_registry};

#[test]
fn every_block_registers_beside_the_kernel_without_a_shared_name() {
    assert_eq!(
        check_registry(&[
            KERNEL_STATEMENTS,
            merezha_application::postgres::STATEMENTS,
            merezha_identity::sql::STATEMENTS,
            merezha_notifications::drivers::STATEMENTS,
            merezha_notifications::sql::STATEMENTS,
            merezha_notifications::stream::STATEMENTS,
            merezha_notifications::push::sql::STATEMENTS,
            merezha_jobs::STATEMENTS,
            merezha_content::STATEMENTS,
            merezha_content::media::STATEMENTS,
        ]),
        Ok(())
    );
}
