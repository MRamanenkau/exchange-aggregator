use serde::Serialize;
use std::error::Error;

#[derive(Debug, Serialize)]
pub struct Kline {
    pub pair: String,
    pub time_frame: String,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub utc_begin: i64,
    pub volume_bs: VBS,
}

#[derive(Debug, Serialize)]
pub struct VBS {
    pub buy_base: f64,
    pub sell_base: f64,
    pub buy_quote: f64,
    pub sell_quote: f64,
}

pub trait KlineParser {
    fn parse(&self, pair: &str, interval: &str, raw_data: Vec<Vec<String>>) -> Result<Vec<Kline>, Box<dyn Error>>;
}

#[derive(Debug)]
pub struct PoloniexKlineParser;

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

        for candle in raw_data { // Iterate over Vec<Vec<String>>
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

        Ok(klines)
    }
}