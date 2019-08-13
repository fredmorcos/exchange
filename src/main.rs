#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{From, Into};
use rust_decimal::{self as decimal, Decimal};
use std::convert::TryFrom;
use std::io::{self, BufRead};
use std::ops::Deref;
use std::str::FromStr;

macro_rules! deref_impl {
    ($src:ty, $dst:ty) => {
        impl Deref for $src {
            type Target = $dst;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, From, Into)]
struct Exchange(String);

deref_impl!(Exchange, String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, From, Into)]
struct Currency(String);

deref_impl!(Currency, String);

#[derive(Debug, Clone, Copy, PartialEq, From, Into)]
struct Factor(Decimal);

deref_impl!(Factor, Decimal);

type Timestamp = DateTime<chrono::FixedOffset>;

#[derive(Debug, Clone, PartialEq)]
struct PriceUpdate {
    timestamp: Timestamp,
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
    InvalidFactors,
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

        let forward_factor =
            Decimal::from_str(forward_factor).map_err(PriceUpdateParseError::ForwardFactor)?;
        let backward_factor =
            Decimal::from_str(backward_factor).map_err(PriceUpdateParseError::BackwardFactor)?;

        if forward_factor * backward_factor > Decimal::new(1, 0) {
            return Err(PriceUpdateParseError::InvalidFactors);
        }

        Ok(Self {
            timestamp: Timestamp::parse_from_rfc3339(timestamp)?,
            exchange: Exchange::from(String::from(*exchange)),
            source_currency: Currency::from(String::from(*source_currency)),
            destination_currency: Currency::from(String::from(*destination_currency)),
            forward_factor: Factor::from(forward_factor),
            backward_factor: Factor::from(backward_factor),
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
    Invalid,
    SourceExchange,
    SourceCurrency,
    DestinationExchange,
    DestinationCurrency,
}

impl TryFrom<&[&str]> for ExchangeRateRequest {
    type Error = ExchangeRateRequestParseError;

    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() > 4 {
            return Err(ExchangeRateRequestParseError::Invalid);
        }

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

#[derive(Debug, From)]
enum ApplicationError {
    IO(io::Error),
    InvalidEvent,
    InvalidExchangeRateRequest(ExchangeRateRequestParseError),
    InvalidPriceUpdate(PriceUpdateParseError),
}

fn main() -> Result<(), ApplicationError> {
    let stdin = io::stdin();
    let mut stdin_handle = stdin.lock();

    let mut buffer = String::new();

    loop {
        if stdin_handle.read_line(&mut buffer)? == 0 {
            statusln!("Closing...");
            break;
        }

        let input: Vec<&str> = buffer.trim().split_whitespace().collect();
        let first_field = input.get(0).ok_or_else(|| ApplicationError::InvalidEvent)?;

        if *first_field == "EXCHANGE_RATE_REQUEST" {
            let _event = ExchangeRateRequest::try_from(&input[1..])?;
        } else {
            let _event = PriceUpdate::try_from(&input[0..])?;
        }

        buffer.clear();
    }

    Ok(())
}
