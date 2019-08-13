#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{From, Into};
use rust_decimal::{self as decimal, Decimal};
use std::convert::TryFrom;
use std::io::{self, BufRead};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, From, Into)]
struct Exchange(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, From, Into)]
struct Currency(String);

#[derive(Debug, Clone, Copy, PartialEq, From, Into)]
struct Factor(Decimal);

#[derive(Debug, Clone, PartialEq)]
struct PriceUpdate {
    timestamp: DateTime<chrono::FixedOffset>,
    exchange: Exchange,
    source_currency: Currency,
    destination_currency: Currency,
    forward_factor: Factor,
    backward_factor: Factor,
}

#[derive(Debug, Clone, From)]
enum PriceUpdateParseError {
    Invalid,
    TimestampMissing,
    Timestamp(chrono::ParseError),
    ExchangeMissing,
    SourceCurrencyMissing,
    DestinationCurrencyMissing,
    ForwardFactorMissing,
    ForwardFactor(decimal::Error),
    BackwardFactorMissing,
    BackwardFactor(decimal::Error),
}

impl TryFrom<&[&str]> for PriceUpdate {
    type Error = PriceUpdateParseError;

    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() > 6 {
            return Err(PriceUpdateParseError::Invalid);
        }

        let timestamp = input
            .get(0)
            .ok_or_else(|| PriceUpdateParseError::TimestampMissing)?;
        let exchange = input
            .get(1)
            .ok_or_else(|| PriceUpdateParseError::ExchangeMissing)?;
        let source_currency = input
            .get(2)
            .ok_or_else(|| PriceUpdateParseError::SourceCurrencyMissing)?;
        let destination_currency = input
            .get(3)
            .ok_or_else(|| PriceUpdateParseError::DestinationCurrencyMissing)?;
        let forward_factor = input
            .get(4)
            .ok_or_else(|| PriceUpdateParseError::ForwardFactorMissing)?;
        let backward_factor = input
            .get(5)
            .ok_or_else(|| PriceUpdateParseError::BackwardFactorMissing)?;

        Ok(Self {
            timestamp: DateTime::parse_from_rfc3339(timestamp)?,
            exchange: Exchange::from(String::from(*exchange)),
            source_currency: Currency::from(String::from(*source_currency)),
            destination_currency: Currency::from(String::from(*destination_currency)),
            forward_factor: Factor::from(
                Decimal::from_str(forward_factor).map_err(PriceUpdateParseError::ForwardFactor)?,
            ),
            backward_factor: Factor::from(
                Decimal::from_str(backward_factor)
                    .map_err(PriceUpdateParseError::BackwardFactor)?,
            ),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ExchangeRateRequest {
    source_exchange: Exchange,
    source_currency: Currency,
    destination_exchange: Exchange,
    destination_currency: Currency,
}

#[derive(Debug, Clone, From)]
enum ExchangeRateRequestParseError {
    SourceExchange,
    SourceCurrency,
    DestinationExchange,
    DestinationCurrency,
}

impl TryFrom<&[&str]> for ExchangeRateRequest {
    type Error = ExchangeRateRequestParseError;

    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        let source_exchange = input
            .get(0)
            .ok_or_else(|| ExchangeRateRequestParseError::SourceExchange)?;
        let source_currency = input
            .get(1)
            .ok_or_else(|| ExchangeRateRequestParseError::SourceCurrency)?;
        let destination_exchange = input
            .get(2)
            .ok_or_else(|| ExchangeRateRequestParseError::DestinationExchange)?;
        let destination_currency = input
            .get(3)
            .ok_or_else(|| ExchangeRateRequestParseError::DestinationCurrency)?;

        Ok(Self {
            source_exchange: Exchange::from(String::from(*source_exchange)),
            source_currency: Currency::from(String::from(*source_currency)),
            destination_exchange: Exchange::from(String::from(*destination_exchange)),
            destination_currency: Currency::from(String::from(*destination_currency)),
        })
    }
}

macro_rules! statusln {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)] {
            eprint!("[STATUS] ");
            eprintln!($($arg)*);
        }
    };
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdin_handle = stdin.lock();

    let mut buffer = String::new();

    loop {
        match stdin_handle.read_line(&mut buffer) {
            Ok(0) => {
                statusln!("Closing...");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: {}, closing...", e);
                break;
            }
        }
    }

    Ok(())
}
