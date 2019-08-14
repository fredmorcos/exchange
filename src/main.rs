#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{Deref, Display, From, Into};
use rust_decimal::{self as decimal, Decimal};
use rust_decimal_macros::dec;
use std::borrow::Cow;
use std::collections::HashMap as Map;
use std::convert::From;
use std::convert::TryFrom;
use std::fmt;
use std::io::{self, BufRead};
use std::str::FromStr;

static DECIMAL_ONE: Decimal = dec!(1.0);

macro_rules! debug_impl {
    ($name:ty) => {
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}({:#?})", stringify!($name), self.0)
            }
        }
    };
}

#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into, Deref)]
struct Exchange<'a>(Cow<'a, str>);

debug_impl!(Exchange<'_>);

#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into, Deref)]
struct Currency<'a>(Cow<'a, str>);

debug_impl!(Currency<'_>);

type Factor = Decimal;
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
struct PriceUpdate<'a> {
    timestamp: Timestamp,
    exchange: Exchange<'a>,
    source_currency: Currency<'a>,
    destination_currency: Currency<'a>,
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
    ForwardFactorInvalid,
    ForwardFactor(decimal::Error),
    BackwardFactorMissing,
    BackwardFactorInvalid,
    BackwardFactor(decimal::Error),
    FactorsInvalid,
}

impl TryFrom<&[&str]> for PriceUpdate<'_> {
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

        if forward_factor * backward_factor > DECIMAL_ONE {
            return Err(PriceUpdateParseError::FactorsInvalid);
        }

        if source_currency == destination_currency {
            if forward_factor != DECIMAL_ONE {
                return Err(PriceUpdateParseError::ForwardFactorInvalid);
            }

            if backward_factor != DECIMAL_ONE {
                return Err(PriceUpdateParseError::BackwardFactorInvalid);
            }
        }

        Ok(Self {
            timestamp: Timestamp::parse_from_rfc3339(timestamp)?,
            exchange: Exchange(Cow::from(String::from(*exchange))),
            source_currency: Currency::from(Cow::from(String::from(*source_currency))),
            destination_currency: Currency::from(Cow::from(String::from(*destination_currency))),
            forward_factor,
            backward_factor,
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
struct ExchangeRateRequest<'a> {
    source_exchange: Exchange<'a>,
    source_currency: Currency<'a>,
    destination_exchange: Exchange<'a>,
    destination_currency: Currency<'a>,
}

#[derive(Debug, Clone, From)]
enum ExchangeRateRequestParseError {
    Invalid,
    SourceExchange,
    SourceCurrency,
    DestinationExchange,
    DestinationCurrency,
}

impl TryFrom<&[&str]> for ExchangeRateRequest<'_> {
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
            source_exchange: Exchange::from(Cow::from(String::from(*source_exchange))),
            source_currency: Currency::from(Cow::from(String::from(*source_currency))),
            destination_exchange: Exchange::from(Cow::from(String::from(*destination_exchange))),
            destination_currency: Currency::from(Cow::from(String::from(*destination_currency))),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Path<'a> {
    exchange: Exchange<'a>,
    currency: Currency<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Info {
    factor: Factor,
    timestamp: Timestamp,
}

#[derive(Debug, Default, Clone, PartialEq)]
struct Graph<'a> {
    exchanges: Map<Exchange<'a>, Map<Currency<'a>, Map<Path<'a>, Info>>>,
}

impl fmt::Display for Graph<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "«")?;

        for (exchange, currencies) in &self.exchanges {
            writeln!(f, "{}", exchange)?;

            for (currency, edge) in currencies {
                for (dst, info) in edge {
                    writeln!(
                        f,
                        "  {} ┈{}⤑ {}({}) @{}",
                        currency, info.factor, dst.exchange, dst.currency, info.timestamp
                    )?;
                }
            }
        }

        write!(f, "»")
    }
}

impl<'a> Graph<'a> {
    fn add_edge(
        &mut self,
        source_exchange: Exchange<'a>,
        source_currency: Currency<'a>,
        destination_exchange: Exchange<'a>,
        destination_currency: Currency<'a>,
        factor: Factor,
        timestamp: Timestamp,
    ) -> &mut Info {
        let destination = Path {
            exchange: destination_exchange,
            currency: destination_currency,
        };

        let info = Info { factor, timestamp };

        self.exchanges
            .entry(source_exchange)
            .or_insert_with(Map::new)
            .entry(source_currency)
            .or_insert_with(Map::new)
            .entry(destination)
            .or_insert_with(|| info)
    }

    fn price_update(&mut self, price_update: PriceUpdate<'a>) {
        let info = self.add_edge(
            price_update.exchange.clone(),
            price_update.source_currency.clone(),
            price_update.exchange.clone(),
            price_update.destination_currency.clone(),
            price_update.forward_factor,
            price_update.timestamp,
        );

        if info.timestamp < price_update.timestamp {
            info.factor = price_update.forward_factor;
            info.timestamp = price_update.timestamp;
        }

        let info = self.add_edge(
            price_update.exchange.clone(),
            price_update.destination_currency.clone(),
            price_update.exchange.clone(),
            price_update.source_currency.clone(),
            price_update.backward_factor,
            price_update.timestamp,
        );

        if info.timestamp < price_update.timestamp {
            info.factor = price_update.backward_factor;
            info.timestamp = price_update.timestamp;
        }

        let other_exchanges: Vec<Exchange> = self
            .exchanges
            .keys()
            .filter(|&e| e != &price_update.exchange.clone())
            .cloned()
            .collect();

        for other_exchange in other_exchanges {
            let info = self.add_edge(
                price_update.exchange.clone(),
                price_update.source_currency.clone(),
                other_exchange.clone(),
                price_update.source_currency.clone(),
                DECIMAL_ONE,
                price_update.timestamp,
            );

            assert_eq!(info.factor, DECIMAL_ONE);

            if info.timestamp < price_update.timestamp {
                info.timestamp = price_update.timestamp;
            }

            let info = self.add_edge(
                price_update.exchange.clone(),
                price_update.destination_currency.clone(),
                other_exchange.clone(),
                price_update.destination_currency.clone(),
                DECIMAL_ONE,
                price_update.timestamp,
            );

            assert_eq!(info.factor, DECIMAL_ONE);

            if info.timestamp < price_update.timestamp {
                info.timestamp = price_update.timestamp;
            }

            let info = self.add_edge(
                other_exchange.clone(),
                price_update.source_currency.clone(),
                price_update.exchange.clone(),
                price_update.source_currency.clone(),
                DECIMAL_ONE,
                price_update.timestamp,
            );

            assert_eq!(info.factor, DECIMAL_ONE);

            if info.timestamp < price_update.timestamp {
                info.timestamp = price_update.timestamp;
            }

            let info = self.add_edge(
                other_exchange,
                price_update.destination_currency.clone(),
                price_update.exchange.clone(),
                price_update.destination_currency.clone(),
                DECIMAL_ONE,
                price_update.timestamp,
            );

            assert_eq!(info.factor, DECIMAL_ONE);

            if info.timestamp < price_update.timestamp {
                info.timestamp = price_update.timestamp;
            }
        }
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
    let mut graph = Graph::default();

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
            graph.price_update(price_update);
        }

        statusln!("{}", graph);

        buffer.clear();
    }

    Ok(())
}
