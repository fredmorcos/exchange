#![warn(clippy::all)]

use chrono::{self, DateTime};
use derive_more::{Display, From, Into};
use rust_decimal::{self as decimal, Decimal};
use rust_decimal_macros::dec;
use std::collections::{HashMap as Map, HashSet as Set};
use std::convert::From;
use std::fmt;
use std::io::{self, BufRead, Write};
use std::rc::Rc;
use std::str::FromStr;

#[derive(Debug, Clone)]
struct StringPool<I> {
    strings: Vec<Rc<String>>,
    indexes: Map<Rc<String>, I>,
}

impl<I> Default for StringPool<I> {
    fn default() -> Self {
        Self {
            strings: Vec::default(),
            indexes: Map::default(),
        }
    }
}

impl<I: Into<usize>> fmt::Display for StringPool<I> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "«")?;
        for (i, v) in self.strings.iter().enumerate() {
            writeln!(f, "  {:<4} ⤑ {}", i, v)?;
        }
        write!(f, "»")
    }
}

impl<I: From<usize> + Copy> StringPool<I> {
    fn add(&mut self, s: String) -> I {
        if let Some(index) = self.indexes.get(&s) {
            *index
        } else {
            let index = I::from(self.strings.len());
            let s = Rc::new(s);
            self.strings.push(s.clone());
            self.indexes.insert(s, index);
            index
        }
    }

    fn add_str(&mut self, s: &str) -> I {
        self.add(String::from(s))
    }

    fn get_index(&self, s: &str) -> Option<I> {
        self.indexes.get(&Rc::new(String::from(s))).copied()
    }

    fn get(&self, index: I) -> &str
    where
        usize: From<I>,
    {
        &self.strings[usize::from(index)]
    }
}

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
#[derive(Display, Clone, Copy, PartialEq, Eq, Hash, From, Into)]
struct Exchange(usize);

debug_impl!(Exchange);

/// A Currency is just a string, but we declare a separate type to avoid potential
/// mistakes (e.g. pass a Currency where an Exchange is expected).
#[derive(Display, Clone, Copy, PartialEq, Eq, Hash, From, Into)]
struct Currency(usize);

debug_impl!(Currency);

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

impl PriceUpdate {
    fn parse(
        input: &[&str],
        exchange_string_pool: &mut StringPool<Exchange>,
        currency_string_pool: &mut StringPool<Currency>,
    ) -> Result<Self, PriceUpdateParseError> {
        // Check that the input has at most 6 fields.
        if input.len() > 6 {
            return Err(PriceUpdateParseError::Invalid);
        }

        // Get each field and if it cannot be found, return the corresponding
        // field-missing error.
        let timestamp = input
            .get(0)
            .ok_or(PriceUpdateParseError::TimestampMissing)?;
        let exchange = input.get(1).ok_or(PriceUpdateParseError::ExchangeMissing)?;
        let source_currency = input
            .get(2)
            .ok_or(PriceUpdateParseError::SourceCurrencyMissing)?;
        let destination_currency = input
            .get(3)
            .ok_or(PriceUpdateParseError::DestinationCurrencyMissing)?;
        let forward_factor = input
            .get(4)
            .ok_or(PriceUpdateParseError::ForwardFactorMissing)?;
        let backward_factor = input
            .get(5)
            .ok_or(PriceUpdateParseError::BackwardFactorMissing)?;

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
            exchange: exchange_string_pool.add_str(exchange),
            source_currency: currency_string_pool.add_str(source_currency),
            destination_currency: currency_string_pool.add_str(destination_currency),
            forward_factor,
            backward_factor,
        })
    }
}

/// Exchange rate requests will come as follows: SRC_E SRC_C DST_E DST_C.
#[derive(Debug, Display, Clone, Copy, PartialEq)]
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
    /// When the input has more than 4 fields.
    Invalid,
    /// If the source exchange field is missing.
    SourceExchangeMissing,
    /// If the source exchange name is unknown
    SourceExchangeUnknown,
    /// If the source currency field is missing.
    SourceCurrencyMissing,
    /// If the source currency name is unknown
    SourceCurrencyUnknown,
    /// If the destination exchange field is missing.
    DestinationExchangeMissing,
    /// If the destination exchange name is unknown
    DestinationExchangeUnknown,
    /// If the destination currency field is missing.
    DestinationCurrencyMissing,
    /// If the destination currency name is unknown
    DestinationCurrencyUnknown,
}

impl ExchangeRateRequest {
    fn parse(
        input: &[&str],
        exchange_string_pool: &mut StringPool<Exchange>,
        currency_string_pool: &mut StringPool<Currency>,
    ) -> Result<Self, ExchangeRateRequestParseError> {
        // Check that the input has at most 4 fields.
        if input.len() > 4 {
            return Err(ExchangeRateRequestParseError::Invalid);
        }

        // Get each field and if it cannot be found, return the corresponding
        // field-missing error.
        let source_exchange = input
            .get(0)
            .ok_or(ExchangeRateRequestParseError::SourceExchangeMissing)?;
        let source_currency = input
            .get(1)
            .ok_or(ExchangeRateRequestParseError::SourceCurrencyMissing)?;
        let destination_exchange = input
            .get(2)
            .ok_or(ExchangeRateRequestParseError::DestinationExchangeMissing)?;
        let destination_currency = input
            .get(3)
            .ok_or(ExchangeRateRequestParseError::DestinationCurrencyMissing)?;

        // Return the constructed exchange rate request structure.
        Ok(Self {
            source_exchange: exchange_string_pool
                .get_index(source_exchange)
                .ok_or(ExchangeRateRequestParseError::SourceExchangeUnknown)?,
            source_currency: currency_string_pool
                .get_index(source_currency)
                .ok_or(ExchangeRateRequestParseError::SourceCurrencyUnknown)?,
            destination_exchange: exchange_string_pool
                .get_index(destination_exchange)
                .ok_or(ExchangeRateRequestParseError::DestinationExchangeUnknown)?,
            destination_currency: currency_string_pool
                .get_index(destination_currency)
                .ok_or(ExchangeRateRequestParseError::DestinationCurrencyUnknown)?,
        })
    }
}

/// A Marker that identifies an (Exchange, Currency) pair. A vertex on the graph.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[display(fmt = "{}({})", exchange, currency)]
struct Marker {
    exchange: Exchange,
    currency: Currency,
}

/// Info is information about an edge: the factor value and the timestamp of the price
/// update that created or last updated it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Info {
    rate: Factor,
    timestamp: Timestamp,
}

/// The Graph is just a list of Exchanges and their corresponding information as follows:
/// an Exchange has a list of Currencies, and each Currency has a list of edges to other
/// (Exchange, Currency) pairs and on each edge there is exchange rate information (the
/// Info structure: factor and timestamp).
#[derive(Debug, Default, Clone, PartialEq)]
struct Graph {
    exchanges: Map<Exchange, Map<Currency, Map<Marker, Info>>>,
    nexts: Map<(Marker, Marker), Marker>,
    rates: Map<(Marker, Marker), Factor>,
}

impl fmt::Display for Graph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "«")?;

        for (exchange, currencies) in &self.exchanges {
            writeln!(f, "{}", exchange)?;

            for (currency, edge) in currencies {
                for (dst, info) in edge {
                    writeln!(
                        f,
                        "  {} ┈{}⤑ {}({}) @{}",
                        currency, info.rate, dst.exchange, dst.currency, info.timestamp
                    )?;
                }
            }
        }

        write!(f, "»")
    }
}

impl Graph {
    /// Insert an edge into the Graph. The edge will be inserted if it is not already in
    /// the graph and a mutable reference to the Info structure comprising of the factor
    /// and timestamp parameters will be returned. However, if the edge is already in the
    /// graph, then a mutable reference to the corresponding Info structure will be
    /// returned instead.
    fn add_edge(
        &mut self,
        source_exchange: Exchange,
        source_currency: Currency,
        destination_exchange: Exchange,
        destination_currency: Currency,
        rate: Factor,
        timestamp: Timestamp,
    ) -> &mut Info {
        // Create the (Exchange, Currency) pair.
        let destination = Marker {
            exchange: destination_exchange,
            currency: destination_currency,
        };

        let info = Info { rate, timestamp };

        // Insert or return the already available Info structure.
        self.exchanges
            .entry(source_exchange)
            .or_insert_with(Map::new)
            .entry(source_currency)
            .or_insert_with(Map::new)
            .entry(destination)
            .or_insert_with(|| info)
    }

    /// Update the graph from a price update event.
    fn price_update(&mut self, price_update: PriceUpdate) {
        // Insert the E:SourceC -> E:DestinationC edge with the forward factor.
        {
            let info = self.add_edge(
                price_update.exchange,
                price_update.source_currency,
                price_update.exchange,
                price_update.destination_currency,
                price_update.forward_factor,
                price_update.timestamp,
            );

            if info.timestamp < price_update.timestamp {
                info.rate = price_update.forward_factor;
                info.timestamp = price_update.timestamp;
            }
        }

        // Insert the E:DestinationC -> E:SourceC edge with the backward factor.
        {
            let info = self.add_edge(
                price_update.exchange,
                price_update.destination_currency,
                price_update.exchange,
                price_update.source_currency,
                price_update.backward_factor,
                price_update.timestamp,
            );

            if info.timestamp < price_update.timestamp {
                info.rate = price_update.backward_factor;
                info.timestamp = price_update.timestamp;
            }
        }

        // The list of all other exchanges.
        let other_exchanges: Vec<Exchange> = self
            .exchanges
            .keys()
            .filter(|&e| e != &price_update.exchange)
            .cloned()
            .collect();

        for other_exchange in other_exchanges {
            // Insert the E:SourceC -> E':SourceC edge with a factor of 1.0.
            {
                let info = self.add_edge(
                    price_update.exchange,
                    price_update.source_currency,
                    other_exchange,
                    price_update.source_currency,
                    DECIMAL_ONE,
                    price_update.timestamp,
                );

                assert_eq!(info.rate, DECIMAL_ONE);

                if info.timestamp < price_update.timestamp {
                    info.timestamp = price_update.timestamp;
                }
            }

            // Insert the E:DestinationC -> E':DestinationC edge with a factor of 1.0
            {
                let info = self.add_edge(
                    price_update.exchange,
                    price_update.destination_currency,
                    other_exchange,
                    price_update.destination_currency,
                    DECIMAL_ONE,
                    price_update.timestamp,
                );

                assert_eq!(info.rate, DECIMAL_ONE);

                if info.timestamp < price_update.timestamp {
                    info.timestamp = price_update.timestamp;
                }
            }

            // Insert the E':SourceC -> E:SourceC edge with a factor of 1.0
            {
                let info = self.add_edge(
                    other_exchange,
                    price_update.source_currency,
                    price_update.exchange,
                    price_update.source_currency,
                    DECIMAL_ONE,
                    price_update.timestamp,
                );

                assert_eq!(info.rate, DECIMAL_ONE);

                if info.timestamp < price_update.timestamp {
                    info.timestamp = price_update.timestamp;
                }
            }

            // Insert the E':DestinationC -> E:DefinationC edge with a factor of 1.0.
            {
                let info = self.add_edge(
                    other_exchange,
                    price_update.destination_currency,
                    price_update.exchange,
                    price_update.destination_currency,
                    DECIMAL_ONE,
                    price_update.timestamp,
                );

                assert_eq!(info.rate, DECIMAL_ONE);

                if info.timestamp < price_update.timestamp {
                    info.timestamp = price_update.timestamp;
                }
            }
        }

        self.update();
    }

    /// Update the rates and next fields in the graph.
    fn update(&mut self) {
        self.nexts.clear();
        self.rates.clear();

        let mut verts: Set<Marker> = Set::new();

        for (source_exchange, source_currencies) in &self.exchanges {
            for (source_currency, destinations) in source_currencies {
                for (destination, info) in destinations {
                    let source = Marker {
                        exchange: *source_exchange,
                        currency: *source_currency,
                    };

                    if let Some(previous) = rates.insert((source, *destination), info.rate) {
                        unreachable!(
                            "Found a redundant entry: {}. It is better to panic here \
                             than to let the program continue running with invalid state.",
                            previous
                        );
                    }

                    if let Some(previous) = self
                        .nexts
                        .insert((source, *destination), destination.clone())
                    {
                        unreachable!(
                            "Found a redundant entry: {}. It is better to panic here \
                             than to let the program continue running with invalid state.",
                            previous
                        );
                    }

                    verts.insert(source);
                    verts.insert(destination.clone());
                }
            }
        }

        for &k in &verts {
            for &i in &verts {
                if i == k {
                    continue;
                }

                let rate_ik = rates.get(&(i, k)).copied();

                for &j in &verts {
                    if i == j || j == k {
                        continue;
                    }

                    let rate_ij = rates.get(&(i, j)).copied();
                    let rate_kj = rates.get(&(k, j)).copied();

                    match (rate_ij, rate_ik, rate_kj) {
                        (_, _, None) | (_, None, _) => {}
                        (None, Some(rate_ik), Some(rate_kj)) => {
                            rates.insert((i, j), rate_ik * rate_kj);

                            let next_ik = if let Some(next_ik) = self.nexts.get(&(i, k)) {
                                *next_ik
                            } else {
                                unreachable!(
                                    "There is a missing nexts entry for (i, k). It is better to panic here \
                                     than to let the program continue running with an invalid graph."
                                );
                            };

                            self.nexts.insert((i, j), next_ik);
                        }
                        (Some(rate_ij), Some(rate_ik), Some(rate_kj)) => {
                            let new_rate = rate_ik * rate_kj;

                            if rate_ij < new_rate {
                                rates.insert((i, j), new_rate);

                                let next_ik = if let Some(next_ik) = self.nexts.get(&(i, k)) {
                                    *next_ik
                                } else {
                                    unreachable!(
                                        "There is a missing nexts entry for (i, k). It is better to panic here \
                                         than to let the program continue running with an invalid graph."
                                    );
                                };

                                self.nexts.insert((i, j), next_ik);
                            }
                        }
                    }
                }
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
        {
            let _res = write!($handle, "[ERROR]  ");
            let _res = writeln!($handle, $($arg)*);
        }
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

    let mut exchange_string_pool = StringPool::default();
    let mut currency_string_pool = StringPool::default();

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
            let exchange_rate_request = match ExchangeRateRequest::parse(
                &input[1..],
                &mut exchange_string_pool,
                &mut currency_string_pool,
            ) {
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
            let price_update = match PriceUpdate::parse(
                &input[0..],
                &mut exchange_string_pool,
                &mut currency_string_pool,
            ) {
                Ok(price_update) => price_update,
                Err(e) => {
                    errorln!(stderr, "Malformed price update: {:#?}", e);
                    continue;
                }
            };

            statusln!(stderr, "{}", price_update);

            // Update the graph with the price update information.
            graph.price_update(price_update);

            statusln!(stderr, "Graph: {}", graph);
            statusln!(stderr, "Exchanges: {}", exchange_string_pool);
            statusln!(stderr, "Currencies: {}", currency_string_pool);
        }

        buffer.clear();
    }

    Ok(())
}
