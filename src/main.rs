use async_std::fs::File;
use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Clap;
use futures::future::join_all;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use xactor::*;
use yahoo_finance_api as yahoo;

#[derive(Clap)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
    #[clap(short, long, default_value = "stocks.csv")]
    output_file: String,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);

    ///
    /// Calculates the absolute and relative difference between the beginning and ending of an f64
    /// series. The relative difference is relative to the beginning.
    ///
    /// # Returns
    ///
    /// A tuple `(absolute, relative)` difference.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

struct WindowedSMA {
    window_size: usize,
}
#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    ///
    /// Window function to create a simple moving average
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

struct MaxPrice {}
#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    ///
    /// Find the maximum in a series of f64
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

struct MinPrice {}
#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    ///
    /// Find the minimum in a series of f64
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are
/// mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();

    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

#[derive(Clone)]
struct StockPeriod {
    symbol: String,
    max: f64,
    min: f64,
    last_price: f64,
    pct_change: f64,
    sma: Vec<f64>,
}

struct StockCalculator {
    price_diff: PriceDifference,
    max_calc: MaxPrice,
    min_calc: MinPrice,
    sma_window: WindowedSMA,
}

impl StockPeriod {
    pub fn print_csv(&self, from: &DateTime<Utc>) {
        // a simple way to output CSV data
        print!("{}", self.format_csv(from));
    }

    pub fn format_csv(&self, from: &DateTime<Utc>) -> String {
        // a simple way to output CSV data
        format!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}\n",
            from.to_rfc3339(),
            self.symbol,
            self.last_price,
            self.pct_change * 100.0,
            self.min,
            self.max,
            self.sma.last().unwrap_or(&0.0)
        )
    }
}

impl StockCalculator {
    pub fn new() -> Self {
        Self {
            price_diff: PriceDifference {},
            max_calc: MaxPrice {},
            min_calc: MinPrice {},
            sma_window: WindowedSMA { window_size: 30 },
        }
    }

    async fn get_stock_data(
        &self,
        symbol: &str,
        from: &DateTime<Utc>,
        to: &DateTime<Utc>,
    ) -> std::io::Result<Option<StockPeriod>> {
        let closes = fetch_closing_data(symbol, &from, &to).await?;
        Ok(self.calc_data(symbol, closes).await)
    }

    async fn calc_data(&self, symbol: &str, closes: Vec<f64>) -> Option<StockPeriod> {
        if !closes.is_empty() {
            // min/max of the period. unwrap() because those are Option types
            let (_, pct_change) = self
                .price_diff
                .calculate(&closes)
                .await
                .unwrap_or((0.0, 0.0));
            Some(StockPeriod {
                symbol: symbol.to_string(),
                max: self.max_calc.calculate(&closes).await.unwrap(),
                min: self.min_calc.calculate(&closes).await.unwrap(),
                last_price: *closes.last().unwrap_or(&0.0),
                pct_change,
                sma: self.sma_window.calculate(&closes).await.unwrap_or_default(),
            })
        } else {
            None
        }
    }
}

#[message(result = "()")]
#[derive(Clone)]
struct StockDownloadMessage {
    symbol_list: Vec<String>,
}

struct StockDownloadActor {
    from: DateTime<Utc>,
}

#[async_trait::async_trait]
impl Actor for StockDownloadActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StockDownloadMessage>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StockDownloadMessage> for StockDownloadActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: StockDownloadMessage) {
        let mut broker = Broker::from_registry().await.unwrap();
        let symbol_list = msg.symbol_list;
        let to = Utc::now();
        let futures: Vec<_> = symbol_list
            .iter()
            .map(|s| fetch_closing_data(s, &self.from, &to))
            .collect();
        let closes_list: Vec<std::io::Result<Vec<f64>>> = join_all(futures).await;

        for (symbol, data) in symbol_list.iter().zip(closes_list.iter()) {
            if let Ok(data) = data {
                broker.publish(StockProcessingMessage {
                    symbol: symbol.clone(),
                    closes: data.clone(),
                });
            }
        }
    }
}

#[message(result = "()")]
#[derive(Clone)]
struct StockProcessingMessage {
    symbol: String,
    closes: Vec<f64>,
}

struct StockProcessingActor {
    from: DateTime<Utc>,
    stock_calculator: StockCalculator,
}

#[async_trait::async_trait]
impl Actor for StockProcessingActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StockProcessingMessage>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StockProcessingMessage> for StockProcessingActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: StockProcessingMessage) {
        let closes = self
            .stock_calculator
            .calc_data(&msg.symbol, msg.closes)
            .await;
        if let Some(closes) = closes {
            let stock_display_msg = StockDisplayMessage { closes };
            Broker::from_registry()
                .await
                .unwrap()
                .publish(stock_display_msg);
        }
    }
}

#[message(result = "()")]
#[derive(Clone)]
struct StockDisplayMessage {
    closes: StockPeriod,
}

struct StockDisplayActor {
    from: DateTime<Utc>,
}

#[async_trait::async_trait]
impl Actor for StockDisplayActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StockDisplayMessage>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StockDisplayMessage> for StockDisplayActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: StockDisplayMessage) {
        msg.closes.print_csv(&self.from);
    }
}

struct StockFileSaveActor {
    from: DateTime<Utc>,
    file: File,
}

#[async_trait::async_trait]
impl Actor for StockFileSaveActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StockDisplayMessage>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StockDisplayMessage> for StockFileSaveActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: StockDisplayMessage) {
        self.file
            .write_all(msg.closes.format_csv(&self.from).as_bytes())
            .await
            .unwrap();
        self.file.sync_all().await.unwrap();
    }
}

#[xactor::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let symbol_list: Vec<&str> = opts.symbols.split(',').collect();
    let mut file = File::create(opts.output_file).await?;
    file.write_all(b"period start,symbol,price,change %,min,max,30d avg\n")
        .await?;
    let stock_downloader = StockDownloadActor { from: from.clone() }.start().await?;
    let stock_processor = StockProcessingActor {
        from: from.clone(),
        stock_calculator: StockCalculator::new(),
    }
    .start()
    .await?;
    let stock_display = StockDisplayActor { from: from.clone() }.start().await?;
    let stock_file_saver = StockFileSaveActor {
        from: from.clone(),
        file,
    }
    .start()
    .await?;
    let mut broker = Broker::from_registry().await?;

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    broker.publish(StockDownloadMessage {
        symbol_list: symbol_list.iter().map(|s| s.to_string()).collect(),
    });
    let mut interval = stream::interval(Duration::from_secs(30));
    while let Some(_) = interval.next().await {
        broker.publish(StockDownloadMessage {
            symbol_list: symbol_list.iter().map(|s| s.to_string()).collect(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
