use serde::Deserialize;
use std::ops::RangeInclusive;

// custom wrapper for parsing ranges
#[derive(Debug, Clone)]
pub struct PortRange(RangeInclusive<u16>);

impl<'de> Deserialize<'de> for PortRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // expect an array of two numbers in the TOML
        let arr: [u16; 2] = Deserialize::deserialize(deserializer)?;
        let start = arr[0];
        let end = arr[1];
        if start > end {
            return Err(serde::de::Error::custom(
                "Range start must not be greater than end",
            ));
        }
        Ok(PortRange(start..=end))
    }
}

// Implement Iterator for PortRange
impl Iterator for PortRange {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

// Implement IntoIterator for PortRange
// impl IntoIterator for PortRange {
//     type Item = u16;
//     type IntoIter = std::ops::RangeInclusive<u16>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.0
//     }
// }
