use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

use deribit::analysis::short_put_history_service::ShortPutHistoryService;
use deribit::config::Config;
use deribit::storage::sqlite::Storage;
use deribit::tui::{self, TuiEvent};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let storage = Storage::new(&config.db_path).await?;

    let (tx, rx) = mpsc::unbounded_channel();

    // Send initial instrument count
    let instrument_count = storage.count_instruments().await.unwrap_or(0);
    if instrument_count > 0 {
        let _ = tx.send(TuiEvent::Connected { instrument_count });
    }

    // Poll DB for new opportunities
    let poll_storage = storage.clone();
    tokio::spawn(async move {
        let mut last_id: i64 = 0;
        let mut poll_interval = interval(Duration::from_secs(2));

        loop {
            poll_interval.tick().await;

            // Update instrument count
            if let Ok(count) = poll_storage.count_instruments().await {
                if count > 0 {
                    let _ = tx.send(TuiEvent::Connected {
                        instrument_count: count,
                    });
                }
            }

            // Load new opportunities
            match poll_storage.load_opportunities_after(last_id).await {
                Ok(entries) => {
                    for (id, opp) in entries {
                        last_id = last_id.max(id);
                        let _ = tx.send(TuiEvent::Opportunity(opp));
                    }
                }
                Err(e) => {
                    eprintln!("DB poll error: {}", e);
                }
            }
        }
    });

    let history_service = ShortPutHistoryService::new(storage.clone(), config.ws_url.clone());

    // Run TUI
    tui::run(rx, Some(history_service)).await?;

    Ok(())
}
