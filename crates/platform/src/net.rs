//! Address helpers for the HTTP edge.
use std::{net::IpAddr, str::FromStr};

/// An IPv4 or IPv6 network such as `10.0.0.0/8` or `::1/128`. A bare
/// address is a single-host network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}
impl Cidr {
    /// Whether `address` lies in this network. IPv4-mapped IPv6 addresses
    /// compare as IPv4.
    pub fn contains(&self, address: IpAddr) -> bool {
        match (self.network, address.to_canonical()) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = u32::MAX
                    .checked_shl(32 - u32::from(self.prefix))
                    .unwrap_or(0);
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = u128::MAX
                    .checked_shl(128 - u32::from(self.prefix))
                    .unwrap_or(0);
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}
/// Rejected network text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidCidr;
impl FromStr for Cidr {
    type Err = InvalidCidr;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = match text.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (text, None),
        };
        let network = IpAddr::from_str(address)
            .map_err(|_| InvalidCidr)?
            .to_canonical();
        let width = if network.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) && p.len() <= 3 => {
                p.parse::<u8>().map_err(|_| InvalidCidr)?
            }
            Some(_) => return Err(InvalidCidr),
            None => width,
        };
        if prefix > width {
            return Err(InvalidCidr);
        }
        Ok(Self { network, prefix })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_matches_networks() {
        let private: Cidr = "10.0.0.0/8".parse().unwrap();
        assert!(private.contains("10.1.2.3".parse().unwrap()));
        assert!(private.contains("::ffff:10.9.9.9".parse().unwrap()));
        assert!(!private.contains("11.0.0.1".parse().unwrap()));
        let host: Cidr = "::1".parse().unwrap();
        assert!(host.contains("::1".parse().unwrap()));
        assert!(!host.contains("::2".parse().unwrap()));
        let all: Cidr = "0.0.0.0/0".parse().unwrap();
        assert!(all.contains("203.0.113.9".parse().unwrap()));
        for bad in ["10.0.0.0/33", "10.0.0.0/", "x/8", "::/129", "10.0.0.0/+8"] {
            assert!(bad.parse::<Cidr>().is_err(), "{bad}");
        }
    }
}
