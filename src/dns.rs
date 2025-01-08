use addr::parse_domain_name;
use hickory_resolver::TokioAsyncResolver;
use serde_derive::Deserialize;
use std::borrow::Cow;
use addr::error::Kind;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use addr::domain::Name;
use serde::{de, Deserialize, Deserializer};
use thiserror::Error;

use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::{AsyncResolver};
use hickory_resolver::error::{ResolveError, ResolveResult};
use hickory_resolver::proto::xfer::FirstAnswer;
use crate::dns::LookupError::NoIp;

static RESOLVER: OnceLock<Arc<TokioAsyncResolver>> = OnceLock::new();

fn init_resolver(config: Option<(ResolverConfig, ResolverOpts)>) -> Arc<TokioAsyncResolver> {
    // you cannot call init twice
    assert!(RESOLVER.get().is_none(), "RESOLVER has already been initialized");

    let init_with= || if let Some((config, options)) = config {
        Arc::new(AsyncResolver::tokio(config, options))
    } else {
        Arc::new(AsyncResolver::tokio_from_system_conf().expect("failed to init dns resolver from system conf"))
    };

    RESOLVER.get_or_init(init_with).clone()
}

fn get_resolver() -> Arc<TokioAsyncResolver> {
    RESOLVER.get_or_init(|| {
        panic!("RESOLVER not initialized, set the resolver with dns::resolve::init_resolver");
    }).clone()
}


#[derive(Error, Debug)]
#[error("invalid address: {s}")]
pub struct HostAddrParse {
    s: String,
}
#[derive(Debug)]
pub enum HostAddr {
    Domain(String),
    Addr(IpAddr),
}

impl FromStr for HostAddr {
    type Err = HostAddrParse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ip) = s.parse::<IpAddr>() {
            Ok(Self::Addr(ip))
        } else if let Ok(name) = parse_domain_name(s) {
            Ok(Self::Domain(s.to_string()))
        } else {
            Err(HostAddrParse { s: s.to_string() })
        }
    }
}

impl FromStr for HostSocket {
    type Err = HostParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // check if the host part is bracketed (for IPv6)
        let (addr, port) = if s.starts_with('[') {
            // handle bracketed IPv6, e.g., "[::1]:8080"
            let end_bracket = s.find(']').ok_or_else(|| HostParseError {
                s: s.to_string(),
            })?;
            if s.len() <= end_bracket + 1 || &s[end_bracket + 1..end_bracket + 2] != ":" {
                return Err(HostParseError { s: s.to_string() });
            }

            // make sure it is an ip if it has square brackets
            let addr = s[1..end_bracket].parse::<IpAddr>().map_err(|e| HostParseError {
                s: s.to_string()
            })?;

            (HostAddr::Addr(addr), &s[end_bracket + 2..])

        } else {
            // handle non-bracketed hosts (IPv4 or domain)
            let (ip, port) = s.rsplit_once(':').ok_or_else(|| HostParseError {
                s: s.to_string(),
            })?;


            // it is not allowed to have an unbracketed/ambiguous IPv6
            if ip.parse::<Ipv6Addr>().is_ok() {
                return Err(HostParseError { s: s.to_string() })
            };
            let addr = ip.parse::<HostAddr>().map_err(|e| HostParseError {
                s: s.to_string()
            })?;

            (addr, port)
        };

        // Parse the port as u16
        let port: u16 = port.parse().map_err(|_| HostParseError {
            s: s.to_string(),
        })?;


        Ok(HostSocket { addr, port })
    }
}

impl<'de> Deserialize<'de> for HostAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        let s: &'de str = <&'de str>::deserialize(deserializer)?;

        s.parse::<Self>().map_err(|e| de::Error::custom(format!(
            "{}", e
        )))
    }
}

impl<'de> Deserialize<'de> for HostSocket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &'de str = <&'de str>::deserialize(deserializer)?;
        s.parse::<Self>().map_err(|e| de::Error::custom(format!("{}", e)))
    }
}


impl HostAddr {
    pub fn name(&self) -> Option<Name<'_>> {
        match self {
            Self::Domain(s) => {
                Some(parse_domain_name(s.as_str()).unwrap())
            }
            _ => None
        }
    }
}

#[derive(Debug)]
pub struct HostSocket {
    addr: HostAddr,
    port: u16
}

#[derive(Error, Debug)]
#[error("invalid host socket: {s}")]
pub struct HostParseError {
    s: String
}

pub trait Resolve: Sized {
    async fn resolve(&self, resolver: &TokioAsyncResolver) -> Result<IpAddr, LookupError>;
}

#[derive(Error, Debug)]
pub enum LookupError {
    #[error("failed to resolve host address")]
    CantResolve(#[from] ResolveError),
    #[error("host address did not resolve to an ip")]
    NoIp
}

impl Resolve for HostAddr {
    async fn resolve(&self, resolver: &TokioAsyncResolver) -> Result<IpAddr, LookupError> {
        match self {
            HostAddr::Domain(name) => {
                resolver.lookup_ip(name).await.map(|ips|  {
                    // prioritise ipv4
                    if let Some(ipv4) = ips.iter().find(|ip| ip.is_ipv4()) {
                        Ok(ipv4)
                    } else if let Some(ipv6) = ips.iter().find(|ip| ip.is_ipv6()) {
                        Ok(ipv6)
                    } else {
                        Err(LookupError::NoIp)
                    }
                })?
            }
            HostAddr::Addr(a) => {Ok(*a)}
        }
    }
}

impl Resolve for HostSocket {
    async fn resolve(&self, resolver: &TokioAsyncResolver) -> Result<IpAddr, LookupError> {
        self.addr.resolve(resolver).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_socket() {
        let input = "127.0.0.1:8080";
        let socket: HostSocket = input.parse().unwrap();
        assert_eq!(socket.port, 8080);
        match socket.addr {
            HostAddr::Addr(IpAddr::V4(ip)) => assert_eq!(ip.to_string(), "127.0.0.1"),
            _ => panic!("Expected IPv4 address"),
        }
    }

    #[test]
    fn test_ipv6_socket() {
        let input = "[::1]:8080";
        let socket: HostSocket = input.parse().unwrap();
        assert_eq!(socket.port, 8080);
        match socket.addr {
            HostAddr::Addr(IpAddr::V6(ip)) => assert_eq!(ip.to_string(), "::1"),
            _ => panic!("Expected IPv6 address"),
        }
    }

    #[test]
    fn test_domain_socket() {
        let input = "example.com:443";
        let socket: HostSocket = input.parse().unwrap();
        assert_eq!(socket.port, 443);
        match socket.addr {
            HostAddr::Domain(domain) => assert_eq!(domain, "example.com"),
            _ => panic!("Expected domain name"),
        }
    }

    #[test]
    fn test_invalid_port() {
        let input = "example.com:port";
        let result: Result<HostSocket, _> = input.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_port() {
        let input = "example.com";
        let result: Result<HostSocket, _> = input.parse();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_ipv6_unbracketed() {
        let input = "2001:db8::1:8080"; // Ambiguous: colon inside address
        let result: Result<HostSocket, _> = input.parse();
        println!("{:?}", result);
        assert!(result.is_err()); // Unbracketed IPv6 with port is invalid
    }

    #[tokio::test]
    async fn test_resolve() {
        // Initialize the resolver with system configuration

        let resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();

        // Test resolving an IPv4 address
        let ipv4_addr = HostAddr::Addr(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        let resolved_ipv4 = ipv4_addr.resolve(&resolver).await.unwrap();
        assert_eq!(resolved_ipv4, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        // Test resolving an IPv6 address
        let ipv6_addr = HostAddr::Addr(IpAddr::V6(Ipv6Addr::LOCALHOST));
        let resolved_ipv6 = ipv6_addr.resolve(&resolver).await.unwrap();
        assert_eq!(resolved_ipv6, IpAddr::V6(Ipv6Addr::LOCALHOST));

        // Test resolving a valid domain name
        let domain_name = HostAddr::Domain("example.com".to_string());
        let resolved_domain = domain_name.resolve(&resolver).await.unwrap();
        println!("Resolved domain to IP: {:?}", resolved_domain);

        // Assert that the resolved IP is valid (may vary depending on DNS settings)
        assert!(resolved_domain.is_ipv4() || resolved_domain.is_ipv6());

        // Test resolving an invalid domain name
        let invalid_domain = HostAddr::Domain("invalid.invalid".to_string());
        let result = invalid_domain.resolve(&resolver).await;
        assert!(matches!(result, Err(LookupError::CantResolve(_))));

        // Test resolving a domain that does not resolve to an IP
        let no_ip_domain = HostAddr::Domain("nonexistent.example.com".to_string());
        let result = no_ip_domain.resolve(&resolver).await;
        assert!(matches!(result, Err(_)));
    }
}
