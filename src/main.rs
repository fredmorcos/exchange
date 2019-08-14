#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{Deref, Display, From, Into};
use rust_decimal::{self as decimal, Decimal};
use rust_decimal_macros::dec;
use std::borrow::Cow;
use std::collections::HashMap as Map;
use std::convert::{From, TryFrom};
use std::fmt;
use std::io::{self, BufRead, Write};
use std::str::FromStr;

/// A decimal 1.0 which we need to reuse a few times.
static DECIMAL_ONE: Decimal = dec!(1.0);

/// Generate a Debug implementation for structs with a single unnamed field.
macro_rules! debug_impl {
    ($name:ty) => {
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}({:#?})", stringify!($name), self.0)
            }
        }
    };
}

/// An Exchange is just a string, but we declare a separate type to avoid potential
/// mistakes (e.g. pass an Exchange where a Currency is expected).
#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into, Deref)]
struct Exchange<'a>(Cow<'a, str>);

debug_impl!(Exchange<'_>);

/// A Currency is just a string, but we declare a separate type to avoid potential
/// mistakes (e.g. pass a Currency where an Exchange is expected).
#[derive(Display, Clone, PartialEq, Eq, Hash, From, Into, Deref)]
struct Currency<'a>(Cow<'a, str>);

debug_impl!(Currency<'_>);

/// A forward or backward factor.
type Factor = Decimal;

/// A timestamp with offset.
type Timestamp = DateTime<chrono::FixedOffset>;

/// Price updates will come as follows: TS EX SRC_C DST_C FW_FAC BW_FAC.
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
    /// When the input has more than 6 fields.
    Invalid,
    /// If the timestamp field is missing.
    TimestampMissing,
    /// If there was an error parsing the timestamp.
    Timestamp(chrono::ParseError),
    /// If the exchange field is missing.
    ExchangeMissing,
    /// If the source currency field is missing.
    SourceCurrencyMissing,
    /// If the destination currency field is missing.
    DestinationCurrencyMissing,
    /// If the forward factor field is missing.
    ForwardFactorMissing,
    /// If there was an error with the forward factor value.
    ForwardFactorInvalid,
    /// If there was an error parsing the forward factor.
    ForwardFactor(decimal::Error),
    /// If the backward factor field is missing.
    BackwardFactorMissing,
    /// If there was an error with the backward factor value.
    BackwardFactorInvalid,
    /// If there was an error parsing the backward factor.
    BackwardFactor(decimal::Error),
    /// If the product of factors is > 1.0.
    FactorsInvalid,
}

impl TryFrom<&[&str]> for PriceUpdate<'_> {
    type Error = PriceUpdateParseError;

    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        // Check that the input has at most 6 fields.
        if input.len() > 6 {
            return Err(PriceUpdateParseError::Invalid);
        }

        // Get each field and if it cannot be found, return the corresponding
        // field-missing error.
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

        // Try to parse the factors and return the corresponding parsing errors.
        let forward_factor =
            Decimal::from_str(forward_factor).map_err(PriceUpdateParseError::ForwardFactor)?;
        let backward_factor =
            Decimal::from_str(backward_factor).map_err(PriceUpdateParseError::BackwardFactor)?;

        // Ensure that the product of factors is <= 1.0.
        if forward_factor * backward_factor > DECIMAL_ONE {
            return Err(PriceUpdateParseError::FactorsInvalid);
        }

        // If the source and destination currencies are the same, both the forward and
        // backward factors _must_ be = 1.0.
        if source_currency == destination_currency {
            if forward_factor != DECIMAL_ONE {
                return Err(PriceUpdateParseError::ForwardFactorInvalid);
            }

            if backward_factor != DECIMAL_ONE {
                return Err(PriceUpdateParseError::BackwardFactorInvalid);
            }
        }

        // Parse what remains and return the corresponding errors on failure, or return
        // the constructed price update structure.
        Ok(Self {
            timestamp: Timestamp::parse_from_rfc3339(timestamp)?,
            exchange: Exchange::from(Cow::from(String::from(*exchange))),
            source_currency: Currency::from(Cow::from(String::from(*source_currency))),
            destination_currency: Currency::from(Cow::from(String::from(*destination_currency))),
            forward_factor,
            backward_factor,
        })
    }
}

/// Exchange rate requests will come as follows: SRC_E SRC_C DST_E DST_C.
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
    /// When the input has more than 4 fields.
    Invalid,
    /// If the source exchange field is missing.
    SourceExchangeMissing,
    /// If the source currency field is missing.
    SourceCurrencyMissing,
    /// If the destination exchange field is missing.
    DestinationExchangeMissing,
    /// If the destination currency field is missing.
    DestinationCurrencyMissing,
}

impl TryFrom<&[&str]> for ExchangeRateRequest<'_> {
    type Error = ExchangeRateRequestParseError;

    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        // Check that the input has at most 4 fields.
        if input.len() > 4 {
            return Err(ExchangeRateRequestParseError::Invalid);
        }

        // Get each field and if it cannot be found, return the corresponding
        // field-missing error.
        let source_exchange = input
            .get(0)
            .ok_or_else(|| ExchangeRateRequestParseError::SourceExchangeMissing)?;
        let source_currency = input
            .get(1)
            .ok_or_else(|| ExchangeRateRequestParseError::SourceCurrencyMissing)?;
        let destination_exchange = input
            .get(2)
            .ok_or_else(|| ExchangeRateRequestParseError::DestinationExchangeMissing)?;
        let destination_currency = input
            .get(3)
            .ok_or_else(|| ExchangeRateRequestParseError::DestinationCurrencyMissing)?;

        // Return the constructed exchange rate request structure.
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

/// Print a status line only in the debug build.
macro_rules! statusln {
    ($handle:ident, $($arg:tt)*) => {
        #[cfg(debug_assertions)] {
            let _res = write!($handle, "[STATUS] ");
            let _res = writeln!($handle, $($arg)*);
        }
    };
}

/// Print an error line.
macro_rules! errorln {
    ($handle:ident, $($arg:tt)*) => {
        let _res = write!($handle, "[ERROR]  ");
        let _res = writeln!($handle, $($arg)*);
    };
}

fn main() -> io::Result<()> {
    // Stdin handle.
    let stdin = io::stdin();
    // Stdin lock handle to avoid locking at each iteration of the main-loop.
    let mut stdin = stdin.lock();

    // Stderr handle.
    let stderr = io::stderr();
    // Stderr lock handle to avoid locking everytime we want to print a status update.
    let mut stderr = stderr.lock();

    // Buffer to read stdin line-wise into.
    let mut buffer = String::new();
    // The graph structure which we either update or query.
    let mut graph = Graph::default();

    loop {
        // Read a line from stdin into the buffer: if an (IO) error occurred then return
        // that, if the input is of length 0 (EOF) then terminate, otherwise continue.
        if stdin.read_line(&mut buffer)? == 0 {
            statusln!(stderr, "Closing...");
            break;
        }

        // Trim the input and split on whitespace.
        let input: Vec<&str> = buffer.trim().split_whitespace().collect();
        // If the input does not have at least 1 field, return to the main-loop.
        let first_field = if let Some(first_field) = input.get(0) {
            first_field
        } else {
            errorln!(stderr, "Malformed input: must contain at least 1 field");
            continue;
        };

        // If the first field says it's an exchange rate request, then attempt to parse it
        // as an exachange rate request and process its query. Otherwise it must be a
        // price update, so attempt to parse it as such and update the graph with the
        // information in it.
        if *first_field == "EXCHANGE_RATE_REQUEST" {
            let exchange_rate_request = match ExchangeRateRequest::try_from(&input[1..]) {
                Ok(exchange_rate_request) => exchange_rate_request,
                Err(e) => {
                    // Could not read the exchange rate request, return to the main-loop.
                    errorln!(stderr, "Malformed exchange rate request: {:#?}", e);
                    continue;
                }
            };

            statusln!(stderr, "{}", exchange_rate_request);
        } else {
            // Could not read the price update, return to the main-loop.
            let price_update = match PriceUpdate::try_from(&input[0..]) {
                Ok(price_update) => price_update,
                Err(e) => {
                    errorln!(stderr, "Malformed price update: {:#?}", e);
                    continue;
                }
            };

            statusln!(stderr, "{}", price_update);

            // Update the graph with the price update information.
            graph.price_update(price_update);
        }

        statusln!(stderr, "{}", graph);

        buffer.clear();
    }

    Ok(())
}
