use tracing::{info, warn};

use crate::analysis::opportunity::Opportunity;
use crate::events::bus::{Event, EventBus};
use crate::storage::sqlite::Storage;

pub struct Notifier {
    storage: Storage,
}

impl Notifier {
    pub fn new(storage: Storage) -> Self {
        Notifier { storage }
    }

    pub async fn run(&self, event_bus: &EventBus) {
        let mut rx = event_bus.subscribe();

        loop {
            match rx.recv().await {
                Ok(Event::OpportunityFound(opp)) => {
                    self.handle_opportunity(&opp).await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "Notifier lagged behind event bus");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    info!("Event bus closed, notifier stopping");
                    break;
                }
                _ => {}
            }
        }
    }

    async fn handle_opportunity(&self, opp: &Opportunity) {
        let separator = "=".repeat(70);
        let thin_sep = "-".repeat(70);

        println!("\n{}", separator);
        println!(
            "  ARBITRAGE: {}  |  Risk: {}",
            opp.strategy_type.to_uppercase(),
            opp.risk_level
        );
        println!("{}", thin_sep);
        println!("  {}", opp.description);
        println!("{}", thin_sep);

        if !opp.legs.is_empty() {
            println!("  EXECUTION STEPS:");
            for leg in &opp.legs {
                println!(
                    "    Step {}: {:4} {} @ {:.6} {} (qty: {:.4})",
                    leg.step, leg.action, leg.instrument, leg.price, leg.price_unit, leg.amount,
                );
            }
            println!("{}", thin_sep);
        }

        if opp.total_cost != 0.0 {
            println!("  Total Cost:     ${:.2}", opp.total_cost);
        }
        if opp.expected_profit > 0.0 {
            println!("  Expected Profit: ${:.2}", opp.expected_profit);
            if opp.total_cost.abs() > 1.0 {
                let roi = (opp.expected_profit / opp.total_cost.abs()) * 100.0;
                println!("  ROI:             {:.2}%", roi);
            }
        }

        println!(
            "  Time: {}",
            chrono::DateTime::from_timestamp(opp.detected_at, 0)
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );
        println!("{}\n", separator);

        if let Err(e) = self.storage.save_opportunity(opp).await {
            warn!(error = %e, "Failed to save opportunity to database");
        }

        info!(
            strategy = %opp.strategy_type,
            profit = opp.expected_profit,
            legs = opp.legs.len(),
            "Opportunity recorded"
        );
    }
}
