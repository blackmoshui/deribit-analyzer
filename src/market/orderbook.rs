use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Single instrument orderbook
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<u64, f64>, // price (as fixed-point) -> amount
    pub asks: BTreeMap<u64, f64>,
    pub change_id: Option<u64>,
    pub prev_change_id: Option<u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            change_id: None,
            prev_change_id: None,
        }
    }

    /// Convert f64 price to fixed-point key (multiply by 10^8 for precision)
    fn price_key(price: f64) -> u64 {
        (price * 1e8) as u64
    }

    /// Apply snapshot (full replacement)
    pub fn apply_snapshot(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        self.bids.clear();
        self.asks.clear();
        for &(price, amount) in bids {
            if amount > 0.0 {
                self.bids.insert(Self::price_key(price), amount);
            }
        }
        for &(price, amount) in asks {
            if amount > 0.0 {
                self.asks.insert(Self::price_key(price), amount);
            }
        }
    }

    /// Apply incremental update
    pub fn apply_update(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        for &(price, amount) in bids {
            let key = Self::price_key(price);
            if amount == 0.0 {
                self.bids.remove(&key);
            } else {
                self.bids.insert(key, amount);
            }
        }
        for &(price, amount) in asks {
            let key = Self::price_key(price);
            if amount == 0.0 {
                self.asks.remove(&key);
            } else {
                self.asks.insert(key, amount);
            }
        }
    }

    /// Get best bid price
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|&k| k as f64 / 1e8)
    }

    /// Get best ask price
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|&k| k as f64 / 1e8)
    }
}

/// Orderbook manager for all instruments
#[derive(Clone)]
pub struct OrderBookManager {
    books: Arc<RwLock<HashMap<String, OrderBook>>>,
}

impl OrderBookManager {
    pub fn new() -> Self {
        OrderBookManager {
            books: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn update(&self, instrument_name: &str, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        let mut books = self.books.write().await;
        let book = books
            .entry(instrument_name.to_string())
            .or_insert_with(OrderBook::new);
        // For simplicity, treat each update as a snapshot
        // TODO: implement proper incremental updates with change_id tracking
        book.apply_snapshot(bids, asks);
    }

    pub async fn get_best_bid_ask(
        &self,
        instrument_name: &str,
    ) -> Option<(Option<f64>, Option<f64>)> {
        let books = self.books.read().await;
        books
            .get(instrument_name)
            .map(|book| (book.best_bid(), book.best_ask()))
    }
}
