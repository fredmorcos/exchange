#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{Display, From, Into};
use rust_decimal::{self as decimal, Decimal};
use std::convert::TryFrom;
use std::fmt;
use std::io::{self, BufRead};
use std::str::FromStr;

macro_rules! deref_impl {
    ($src:ty, $dst:ty) => {
        impl std::ops::Deref for $src {
            type Target = $dst;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

macro_rules! debug_impl {
    ($name:ty) => {
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}({:#?})", stringify!($name), self.0)
            }
        }
    };
}

#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into)]
struct Exchange(String);

deref_impl!(Exchange, String);
debug_impl!(Exchange);

#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into)]
struct Currency(String);

deref_impl!(Currency, String);
debug_impl!(Currency);

#[derive(Display, Clone, Copy, PartialEq, From, Into)]
struct Factor(Decimal);

deref_impl!(Factor, Decimal);
debug_impl!(Factor);

type Timestamp = DateTime<chrono::FixedOffset>;

#[derive(Debug, Display, Clone, PartialEq)]
#[display(
    fmt = "PRICE UPDATE {} {} {} {} {} {}",
    timestamp,
    exchange,
    source_currency,
    destination_currency,
    forward_factor,
    backward_factor
)]
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

#[derive(Debug, Display, Clone, PartialEq)]
#[display(
    fmt = "EXCHANGE RATE REQUEST {}({}) {}({})",
    source_exchange,
    source_currency,
    destination_exchange,
    destination_currency
)]
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
            let exchange_rate_request = ExchangeRateRequest::try_from(&input[1..])?;
            statusln!("{}", exchange_rate_request);
        } else {
            let price_update = PriceUpdate::try_from(&input[0..])?;
            statusln!("{}", price_update);
        }

        buffer.clear();
    }

    Ok(())
}
