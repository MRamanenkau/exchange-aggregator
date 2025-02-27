use std::error::Error;
use super::{Kline, VBS};

#[derive(Debug)]
struct Kline {
    pair: String,      // Trading pair
    time_frame: String,// Candle timeframe (1m, 15m, 1h, 1d)
    o: f64,            // Open price
    h: f64,            // High price
    l: f64,            // Low price
    c: f64,            // Close price
    utc_begin: i64,    // Unix time of candle start
    volume_bs: VBS,    // Volume data
}

#[derive(Debug)]
struct VBS {
    buy_base: f64,     // Buy volume in base currency
    sell_base: f64,    // Sell volume in base currency
    buy_quote: f64,    // Buy volume in quote currency
    sell_quote: f64,   // Sell volume in quote currency
}

trait KlineParser {
    fn parse(&self, pair: &str, interval: &str, raw_data: Vec<Vec<String>>) -> Result<Vec<Kline>, Box<dyn Error>>;
}

#[derive(Debug)]
struct PoloniexKlineParser;

impl KlineParser for PoloniexKlineParser {
    fn parse(&self, pair: &str, interval: &str, raw_data: Vec<Vec<String>>) -> Result<Vec<Kline>, Box<dyn Error>> {
        let time_frame = match interval {
            "MINUTE_5" => "5m".to_string(),
            "MINUTE_15" => "15m".to_string(),
            "HOUR_1" => "1h".to_string(),
            "DAY_1" => "1d".to_string(),
            _ => unreachable!(),
        };

        let mut klines = Vec::new();

        for batch in raw_data {
            for candle in batch {
                // Assuming the order matches KlineRaw:
                // [low, high, open, close, amount, quantity, buyTakerAmount, buyTakerQuantity, ..., startTime, closeTime]
                let total_base = candle[5].parse::<f64>()?;
                let total_quote = candle[4].parse::<f64>()?;
                let buy_base = candle[7].parse::<f64>()?;
                let buy_quote = candle[6].parse::<f64>()?;
                let utc_begin = candle[12].parse::<i64>()?;

                let kline = Kline {
                    pair: pair.to_string(),
                    time_frame: time_frame.clone(),
                    l: candle[0].parse()?,
                    h: candle[1].parse()?,
                    o: candle[2].parse()?,
                    c: candle[3].parse()?,
                    utc_begin,
                    volume_bs: VBS {
                        buy_base,
                        sell_base: total_base - buy_base,
                        buy_quote,
                        sell_quote: total_quote - buy_quote,
                    },
                };
                klines.push(kline);
            }
        }

        Ok(klines)
    }
}