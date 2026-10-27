//! Block selection (scaffold §2.2).
use crate::Error;
use std::{collections::BTreeSet, fmt};

/// A block a project may switch on. The kernel, jobs and the PostgreSQL
/// application are always on. The order is the canonical one, used in
/// reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Block {
    Identity,
    Notifications,
    Content,
    Telemetry,
}

impl Block {
    pub const ALL: [Block; 4] = [
        Block::Identity,
        Block::Notifications,
        Block::Content,
        Block::Telemetry,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Block::Identity => "identity",
            Block::Notifications => "notifications",
            Block::Content => "content",
            Block::Telemetry => "telemetry",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|block| block.as_str() == name)
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A set of blocks that satisfies the rules of §2.2.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Selection(BTreeSet<Block>);

impl Selection {
    /// Every block.
    pub fn all() -> Self {
        Self(Block::ALL.into_iter().collect())
    }

    /// `--blocks LIST`: comma-separated block names, or `none`. A name that
    /// is not a block (an empty one included) is `unknown_block`; a block
    /// named twice, or `none` beside a block, `invalid_arguments`. The
    /// result must pass [`Selection::checked`].
    pub fn parse(list: &str) -> Result<Self, Error> {
        if list == "none" {
            return Ok(Self::default());
        }
        let mut blocks = BTreeSet::new();
        for name in list.split(',') {
            if name == "none" {
                return Err(Error::invalid("invalid_arguments"));
            }
            let block = Block::parse(name).ok_or_else(|| Error::invalid("unknown_block"))?;
            if !blocks.insert(block) {
                return Err(Error::invalid("invalid_arguments"));
            }
        }
        Self(blocks).checked()
    }

    /// The selection if every block it holds can start (§2.2):
    /// `identity` needs `notifications` (`block_requires_notifications`) and
    /// `content` needs `identity` (`block_requires_identity`).
    pub fn checked(self) -> Result<Self, Error> {
        if self.contains(Block::Identity) && !self.contains(Block::Notifications) {
            return Err(Error::invalid("block_requires_notifications"));
        }
        if self.contains(Block::Content) && !self.contains(Block::Identity) {
            return Err(Error::invalid("block_requires_identity"));
        }
        Ok(self)
    }

    pub fn contains(&self, block: Block) -> bool {
        self.0.contains(&block)
    }

    pub fn iter(&self) -> impl Iterator<Item = Block> + '_ {
        self.0.iter().copied()
    }

    /// The block names, in canonical order.
    pub fn names(&self) -> Vec<&'static str> {
        self.iter().map(Block::as_str).collect()
    }

    /// Every selection §2.2 allows, `none` first.
    pub fn allowed() -> Vec<Self> {
        (0u8..16)
            .filter_map(|bits| {
                let blocks = Block::ALL
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| bits & (1 << i) != 0)
                    .map(|(_, block)| block)
                    .collect();
                Self(blocks).checked().ok()
            })
            .collect()
    }
}

impl FromIterator<Block> for Selection {
    /// An unchecked selection; pass it through [`Selection::checked`].
    fn from_iter<I: IntoIterator<Item = Block>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
